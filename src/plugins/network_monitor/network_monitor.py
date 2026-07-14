import logging
import os
import re
import socket
import sqlite3
import struct
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta

import pytz
from PIL import Image, ImageDraw

from plugins.base_plugin.base_plugin import BasePlugin
from utils.app_utils import get_font

logger = logging.getLogger(__name__)

# Failover hierarchy: try each external resolver in order and stop at the first
# one that answers, so one provider dropping ICMP doesn't read as a real outage.
# Cloudflare first (fast anycast backbone check), Google as the standard/most
# widely-reachable fallback, Quad9 as a second, independently-routed fallback.
EXTERNAL_HOSTS = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
PING_INTERVAL_SECONDS = 1.0
PING_TIMEOUT_SECONDS = 1.0
# Used only as a connectivity/latency probe when raw ICMP sockets aren't available
# (non-root, or non-Linux dev environments) - see _PingPoller._probe_host.
TCP_FALLBACK_PORT = 53

# Only probed when every external host above has already failed, to tell apart a
# local network/Wi-Fi problem (gateway also unreachable) from an ISP/backbone
# outage (gateway fine, nothing external answers). Not on the hot path, so it's
# fine that this is a little more expensive than the external-host probe.
GATEWAY_PROBE_TIMEOUT_SECONDS = 1.0
GATEWAY_TCP_PROBE_PORT = 80
GATEWAY_REDETECT_INTERVAL_SECONDS = 300

FLUSH_INTERVAL_SECONDS = 5
FLUSH_BATCH_SIZE = 30
# WAL mode normally auto-checkpoints on its own, but this is a 24/7 writer on a
# Pi's SD card - an explicit periodic checkpoint bounds the WAL file's on-disk
# size instead of trusting that nothing ever delays the automatic one.
WAL_CHECKPOINT_INTERVAL_SECONDS = 60
# Raw 1Hz samples are only needed to draw the 12h sparkline, so they're pruned far
# sooner than the outage/gap episode tables, which back the 7-day histogram and
# 4-week calendar and are tiny (one row per outage/restart, not one per second).
RAW_RETENTION_SECONDS = 14 * 3600
# Covers the calendar's fixed 4 weeks plus headroom for the histogram's
# configurable history-weeks setting (capped at MAX_HISTORY_WEEKS).
EPISODE_RETENTION_SECONDS = 95 * 24 * 3600
MAX_HISTORY_WEEKS = 12
# A restart/reboot is detected by comparing "now" to the last recorded sample at
# startup; a small slack avoids flagging the normal ~1s gap between ticks.
STARTUP_GAP_THRESHOLD_SECONDS = 5

# Fixed 6-color e-ink palette - all fills below are flat colors from this set,
# deliberately with no gradients/alpha blending, which don't survive e-ink dithering.
COLOR_BLACK = "#000000"
COLOR_WHITE = "#ffffff"
COLOR_RED = "#a6242b"
COLOR_YELLOW = "#dfaf2c"
COLOR_BLUE = "#345f94"
COLOR_GREEN = "#428c46"

DEFAULT_TITLE = "Internet Outage Monitor"


def _detect_default_gateway():
    """Best-effort local default-gateway lookup, used only for the local-vs-ISP
    outage diagnostic. Returns None if it can't be determined (unsupported
    platform, no default route, parsing failure, etc.) - the plugin still works
    without it, it just can't tell a local outage apart from an ISP one."""
    if sys.platform.startswith("linux"):
        try:
            with open("/proc/net/route") as f:
                for line in f.readlines()[1:]:
                    fields = line.split()
                    if len(fields) >= 3 and fields[1] == "00000000":
                        return socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
        except Exception:
            logger.debug("Could not read default gateway from /proc/net/route", exc_info=True)
        return None

    try:
        if os.name == "nt":
            output = subprocess.run(["ipconfig"], capture_output=True, text=True, timeout=3).stdout
            match = re.search(r"Default Gateway[ .:]+([\d.]+)", output)
        else:
            output = subprocess.run(["netstat", "-rn"], capture_output=True, text=True, timeout=3).stdout
            match = re.search(r"^default\s+(\S+)", output, re.MULTILINE)
        return match.group(1) if match else None
    except Exception:
        logger.debug("Could not determine default gateway", exc_info=True)
        return None


class _PingStore:
    """Owns the sqlite database of ping samples and outage/gap episodes.

    Shared between the background poller thread (writer) and generate_image
    (reader, possibly called from a different thread). Each thread gets its own
    connection since sqlite3 connections aren't safe to share across threads.
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self._local = threading.local()
        self._last_checkpoint_mono = 0.0
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        conn = self._connect()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS pings (
                ts INTEGER PRIMARY KEY,
                rtt_ms REAL
            );
            CREATE TABLE IF NOT EXISTS outages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER
            );
            CREATE TABLE IF NOT EXISTS gaps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('first_started_ts', ?)",
            (str(int(time.time())),),
        )
        conn.commit()

    def _connect(self):
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    def record_startup_gap(self):
        conn = self._connect()
        last_ts = conn.execute("SELECT MAX(ts) FROM pings").fetchone()[0]
        now = int(time.time())
        if last_ts is not None and now - last_ts > STARTUP_GAP_THRESHOLD_SECONDS:
            conn.execute("INSERT INTO gaps(start_ts, end_ts) VALUES (?, ?)", (last_ts, now))
            conn.commit()
            logger.info("Detected a %ds gap in polling (restart/reboot); recorded as a gap episode.", now - last_ts)

    def flush_pings(self, rows):
        if not rows:
            return
        conn = self._connect()
        conn.executemany("INSERT OR IGNORE INTO pings(ts, rtt_ms) VALUES (?, ?)", rows)
        conn.commit()

    def insert_outage(self, start_ts, end_ts):
        conn = self._connect()
        conn.execute("INSERT INTO outages(start_ts, end_ts) VALUES (?, ?)", (start_ts, end_ts))
        conn.commit()

    def prune(self, now):
        conn = self._connect()
        conn.execute("DELETE FROM pings WHERE ts < ?", (now - RAW_RETENTION_SECONDS,))
        conn.execute(
            "DELETE FROM outages WHERE end_ts IS NOT NULL AND end_ts < ?",
            (now - EPISODE_RETENTION_SECONDS,),
        )
        conn.execute("DELETE FROM gaps WHERE end_ts < ?", (now - EPISODE_RETENTION_SECONDS,))
        conn.commit()
        self._maybe_checkpoint(conn)

    def _maybe_checkpoint(self, conn):
        now_mono = time.monotonic()
        if now_mono - self._last_checkpoint_mono < WAL_CHECKPOINT_INTERVAL_SECONDS:
            return
        self._last_checkpoint_mono = now_mono
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            logger.debug("WAL checkpoint failed", exc_info=True)

    def has_any_data(self):
        conn = self._connect()
        return conn.execute("SELECT 1 FROM pings LIMIT 1").fetchone() is not None

    def get_samples(self, since_ts, until_ts):
        conn = self._connect()
        rows = conn.execute(
            "SELECT ts, rtt_ms FROM pings WHERE ts >= ? AND ts <= ? ORDER BY ts", (since_ts, until_ts)
        ).fetchall()
        return rows

    def get_gaps(self, since_ts, until_ts):
        conn = self._connect()
        return conn.execute(
            "SELECT start_ts, end_ts FROM gaps WHERE end_ts >= ? AND start_ts <= ?", (since_ts, until_ts)
        ).fetchall()

    def get_outages(self, since_ts, until_ts, now_ts):
        conn = self._connect()
        rows = conn.execute(
            "SELECT start_ts, end_ts FROM outages WHERE (end_ts IS NULL OR end_ts >= ?) AND start_ts <= ?",
            (since_ts, until_ts),
        ).fetchall()
        return [(start, end if end is not None else now_ts) for start, end in rows]

    def get_meta_int(self, key, default):
        conn = self._connect()
        row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return int(row[0]) if row else default


class _PingPoller(threading.Thread):
    """Background thread that probes EXTERNAL_HOSTS roughly once a second, forever,
    persisting samples and derived outage episodes to the shared _PingStore."""

    def __init__(self, store, gateway_override=None):
        super().__init__(daemon=True, name="network-monitor-poller")
        self.store = store
        self.gateway_override = gateway_override
        self._stop_event = threading.Event()
        self._icmp_usable = None  # None = untested, True/False once known

        self.gateway_ip = None
        self._gateway_checked_at = 0.0

        self.last_status = "unknown"
        self.last_rtt_ms = None
        self.last_sample_ts = None
        self.last_host = None  # which external host most recently answered
        self.last_outage_scope = None  # 'local' | 'isp' | 'unknown', set only while last_status == 'outage'

    def stop(self):
        self._stop_event.set()

    def run(self):
        try:
            self.store.record_startup_gap()
        except Exception:
            logger.exception("Failed to check for a startup polling gap")

        buffer = []
        outage_start_ts = None
        last_flush = time.monotonic()

        while not self._stop_event.is_set():
            loop_start = time.monotonic()
            try:
                # Everything for one tick lives in this try block as a last-resort
                # safety net: any single unhandled exception here would otherwise
                # silently kill the whole background thread, and nothing else
                # watches for that (see NetworkMonitor._ensure_poller_started,
                # which only restarts a poller it can see has actually died).
                try:
                    rtt_ms = self._ping_once()
                except Exception:
                    logger.exception("Unexpected error while pinging %s", EXTERNAL_HOSTS)
                    rtt_ms = None
                ts = int(time.time())

                buffer.append((ts, rtt_ms))
                self.last_sample_ts = ts
                self.last_rtt_ms = rtt_ms
                self.last_status = "online" if rtt_ms is not None else "outage"

                if rtt_ms is None:
                    if outage_start_ts is None:
                        outage_start_ts = ts
                        logger.warning(
                            "Internet outage detected (all of %s unreachable; scope=%s).",
                            EXTERNAL_HOSTS,
                            self.last_outage_scope,
                        )
                elif outage_start_ts is not None:
                    try:
                        self.store.insert_outage(outage_start_ts, ts)
                    except Exception:
                        logger.exception("Failed to record outage episode")
                    logger.warning(
                        "Internet outage resolved after %ds (answered by %s).", ts - outage_start_ts, self.last_host
                    )
                    outage_start_ts = None

                now_mono = time.monotonic()
                if buffer and (len(buffer) >= FLUSH_BATCH_SIZE or now_mono - last_flush >= FLUSH_INTERVAL_SECONDS):
                    try:
                        self.store.flush_pings(buffer)
                        self.store.prune(int(time.time()))
                    except Exception:
                        logger.exception("Failed to flush/prune ping data")
                    buffer = []
                    last_flush = now_mono
            except Exception:
                logger.exception("Unexpected error in internet outage monitor poll loop; continuing")

            elapsed = time.monotonic() - loop_start
            self._stop_event.wait(max(0.0, PING_INTERVAL_SECONDS - elapsed))

        # best-effort flush on shutdown so the last few seconds aren't lost
        try:
            self.store.flush_pings(buffer)
            if outage_start_ts is not None:
                self.store.insert_outage(outage_start_ts, int(time.time()))
        except Exception:
            logger.exception("Failed to flush ping data on shutdown")

    def _ping_once(self):
        # Failover hierarchy: try each external host in order, first answer wins.
        # This is the "Step 2 / Step 3" logic - only fall through to the next host
        # if the previous one actually failed to answer.
        for host in EXTERNAL_HOSTS:
            rtt = self._probe_host(host, PING_TIMEOUT_SECONDS)
            if rtt is not None:
                self.last_host = host
                self.last_outage_scope = None
                return rtt

        # Every external host failed - this is "Step 4": before declaring an
        # outage, check whether the local gateway is even reachable, to tell a
        # local network/Wi-Fi/cable problem apart from an ISP/backbone outage.
        self._ensure_gateway_detected()
        if self.gateway_ip:
            self.last_outage_scope = "isp" if self._probe_gateway_reachable(self.gateway_ip) else "local"
        else:
            self.last_outage_scope = "unknown"
        self.last_host = None
        return None

    def _ensure_gateway_detected(self):
        if self.gateway_override:
            self.gateway_ip = self.gateway_override
            return
        now = time.monotonic()
        if self.gateway_ip is not None and now - self._gateway_checked_at < GATEWAY_REDETECT_INTERVAL_SECONDS:
            return
        self.gateway_ip = _detect_default_gateway()
        self._gateway_checked_at = now
        if self.gateway_ip:
            logger.debug("Using %s as the local gateway for outage-scope diagnostics.", self.gateway_ip)

    def _probe_gateway_reachable(self, host):
        """Boolean-only reachability check for the local gateway (routers rarely
        run a DNS resolver, so this isn't reused for latency/EXTERNAL_HOSTS probing).
        A TCP connection being actively refused still counts as reachable - it means
        something at that address answered our packet, just not on that port."""
        rtt = self._probe_host(host, GATEWAY_PROBE_TIMEOUT_SECONDS)
        if rtt is not None:
            return True
        try:
            with socket.create_connection((host, GATEWAY_TCP_PROBE_PORT), timeout=GATEWAY_PROBE_TIMEOUT_SECONDS):
                return True
        except ConnectionRefusedError:
            return True
        except OSError:
            return False

    def _probe_host(self, host, timeout):
        # Real ICMP echo needs raw sockets (root/CAP_NET_RAW), which the InkyPi
        # service has on the Pi (runs as root); anywhere that's not available
        # (dev machines, containers) it falls back to timing a TCP connect to
        # the same host, which is still a meaningful up/down + latency probe.
        if sys.platform.startswith("linux") and self._icmp_usable is not False:
            rtt = self._icmp_ping(host, timeout)
            if rtt is not NotImplemented:
                self._icmp_usable = True
                return rtt
            self._icmp_usable = False
            logger.info("Raw ICMP ping unavailable (needs root/CAP_NET_RAW); falling back to TCP connect timing.")
        return self._tcp_ping(host, timeout)

    def _icmp_ping(self, host, timeout):
        """Returns RTT in ms, None on timeout/no-reply, or NotImplemented if raw
        ICMP sockets can't be used here (caller falls back to TCP)."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_ICMP)
        except (PermissionError, OSError):
            return NotImplemented

        try:
            sock.settimeout(timeout)
            packet_id = os.getpid() & 0xFFFF
            payload = struct.pack("d", time.time())
            header = struct.pack("!BBHHH", 8, 0, 0, packet_id, 1)
            checksum = self._icmp_checksum(header + payload)
            header = struct.pack("!BBHHH", 8, 0, checksum, packet_id, 1)
            packet = header + payload

            send_time = time.time()
            try:
                sock.sendto(packet, (host, 0))
            except OSError:
                return None

            deadline = send_time + timeout
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    return None
                sock.settimeout(remaining)
                try:
                    reply, addr = sock.recvfrom(1024)
                except (socket.timeout, OSError):
                    return None
                recv_time = time.time()
                if addr[0] != host or len(reply) < 20:
                    continue
                ip_header_len = (reply[0] & 0x0F) * 4
                icmp_reply = reply[ip_header_len:ip_header_len + 8]
                if len(icmp_reply) < 8:
                    continue
                reply_type, _reply_code, _checksum, reply_id, _reply_seq = struct.unpack("!BBHHH", icmp_reply)
                if reply_type == 0 and reply_id == packet_id:
                    return (recv_time - send_time) * 1000.0
        finally:
            sock.close()

    @staticmethod
    def _icmp_checksum(data):
        if len(data) % 2:
            data += b"\x00"
        total = sum(struct.unpack("!%dH" % (len(data) // 2), data))
        total = (total >> 16) + (total & 0xFFFF)
        total += total >> 16
        return ~total & 0xFFFF

    def _tcp_ping(self, host, timeout):
        start = time.time()
        try:
            with socket.create_connection((host, TCP_FALLBACK_PORT), timeout=timeout):
                return (time.time() - start) * 1000.0
        except OSError:
            return None


class NetworkMonitor(BasePlugin):
    def __init__(self, config, **dependencies):
        super().__init__(config, **dependencies)
        self._store = None
        self._poller = None
        self._start_lock = threading.Lock()

    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        return template_params

    def on_startup(self, instance_settings, device_config):
        # Only eagerly start at app boot if the user opted in for this instance;
        # otherwise monitoring still starts lazily on first view/refresh (see
        # generate_image), so plugins nobody ever added stay fully idle.
        if self._is_persistent(instance_settings):
            self._ensure_poller_started(self._parse_gateway_override(instance_settings))

    @staticmethod
    def _is_persistent(settings):
        return str(settings.get("persistentMonitoring", "")).lower() == "true"

    @staticmethod
    def _parse_gateway_override(settings):
        value = (settings.get("gatewayIp") or "").strip()
        return value or None

    def generate_image(self, settings, device_config):
        self._ensure_poller_started(self._parse_gateway_override(settings))

        title = (settings.get("title") or "").strip() or DEFAULT_TITLE

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        timezone_name = device_config.get_config("timezone", default="America/New_York")
        time_format = device_config.get_config("time_format", default="12h")
        tz = pytz.timezone(timezone_name)
        now_dt = datetime.now(tz)
        now_ts = int(now_dt.timestamp())

        width, height = dimensions
        fonts = {
            "title": get_font("Jost", round(height * 0.040), "bold"),
            "label": get_font("Jost", round(height * 0.020), "bold"),
            "small": get_font("Jost", round(height * 0.014), "normal"),
        }

        if not self._store.has_any_data():
            return self._render_waiting_image(dimensions, title, fonts)

        image = Image.new("RGB", dimensions, COLOR_WHITE)
        draw = ImageDraw.Draw(image)

        title_height = round(height * 0.075)
        self._draw_title(draw, width, title_height, title, now_dt, fonts)

        body_top = title_height
        margin = round(width * 0.012)
        left_width = round(width * 2 / 3)

        left_box = (margin, body_top + margin, left_width - margin // 2, height - margin)
        right_top = body_top + margin
        right_bottom = height - margin
        right_height = right_bottom - right_top
        histogram_box = (left_width + margin // 2, right_top, width - margin, right_top + round(right_height * 0.42))
        calendar_box = (left_width + margin // 2, right_top + round(right_height * 0.42) + margin, width - margin, right_bottom)

        min_outage_seconds = self._parse_min_outage_seconds(settings)
        history_weeks = self._parse_history_weeks(settings)
        week_start = self._parse_week_start(settings)
        aggregation = self._parse_aggregation(settings)
        self._draw_sparklines(draw, left_box, now_ts, tz, fonts, min_outage_seconds, time_format)
        self._draw_histogram(draw, histogram_box, now_ts, tz, fonts, min_outage_seconds, history_weeks, week_start, aggregation)
        self._draw_calendar(draw, calendar_box, now_ts, tz, fonts, min_outage_seconds)

        return image

    @staticmethod
    def _parse_min_outage_seconds(settings):
        try:
            value = int(settings.get("minOutageSeconds"))
        except (TypeError, ValueError):
            value = 2
        return max(0, value)

    @staticmethod
    def _parse_history_weeks(settings):
        try:
            value = int(settings.get("historyWeeks"))
        except (TypeError, ValueError):
            value = 4
        return max(1, min(MAX_HISTORY_WEEKS, value))

    @staticmethod
    def _parse_week_start(settings):
        return "monday" if str(settings.get("weekStart", "")).strip().lower() == "monday" else "sunday"

    @staticmethod
    def _parse_aggregation(settings):
        value = str(settings.get("aggregation", "")).strip().lower()
        return value if value in ("max", "min", "average") else "average"

    @staticmethod
    def _filter_outages(outages, min_seconds):
        if min_seconds <= 0:
            return outages
        return [(start, end) for start, end in outages if (end - start) >= min_seconds]

    def _ensure_poller_started(self, gateway_override=None):
        # Checks is_alive(), not just "is None": a poller that has died (crash,
        # thread killed, whatever) would otherwise leave self._poller set forever,
        # and every future render/on_startup call would wrongly treat monitoring
        # as already running and never bring it back.
        if self._poller is not None and self._poller.is_alive():
            return
        with self._start_lock:
            if self._poller is not None and self._poller.is_alive():
                return
            if self._poller is not None:
                logger.warning("Internet outage monitor poller had stopped unexpectedly; restarting it.")
            if self._store is None:
                db_path = os.path.join(self.get_plugin_dir("data"), "pings.db")
                self._store = _PingStore(db_path)
            self._poller = _PingPoller(self._store, gateway_override=gateway_override)
            self._poller.start()
            logger.info(
                "Started internet outage monitor background poller (probing %s roughly every %.0fs).",
                EXTERNAL_HOSTS,
                PING_INTERVAL_SECONDS,
            )

    # ---- rendering -------------------------------------------------------

    def _render_waiting_image(self, dimensions, title, fonts):
        width, height = dimensions
        image = Image.new("RGB", dimensions, COLOR_WHITE)
        draw = ImageDraw.Draw(image)
        draw.text((width / 2, height / 2 - 20), title, font=fonts["title"], fill=COLOR_BLACK, anchor="mm")
        draw.text(
            (width / 2, height / 2 + 30),
            "Collecting data... check back in a minute.",
            font=fonts["label"],
            fill=COLOR_BLUE,
            anchor="mm",
        )
        return image

    def _current_status(self):
        poller = self._poller
        if poller is None or poller.last_status == "unknown":
            return "Unknown", COLOR_YELLOW
        if poller.last_status == "online":
            return "Online", COLOR_GREEN
        if poller.last_outage_scope == "local":
            return "Outage (Local)", COLOR_RED
        if poller.last_outage_scope == "isp":
            return "Outage (ISP)", COLOR_RED
        return "Outage", COLOR_RED

    def _draw_title(self, draw, width, title_height, title, now_dt, fonts):
        pad = round(width * 0.0125)
        draw.line([(0, title_height), (width, title_height)], fill=COLOR_BLACK, width=3)
        draw.text((pad, title_height / 2), title, font=fonts["title"], fill=COLOR_BLACK, anchor="lm")

        status_text, status_color = self._current_status()
        badge_font = fonts["label"]
        text_w = draw.textlength(status_text, font=badge_font)
        badge_pad_x, badge_pad_y = 20, 10
        badge_w = text_w + badge_pad_x * 2
        badge_h = badge_font.size + badge_pad_y * 2
        badge_right = width - pad
        badge_left = badge_right - badge_w
        badge_top = (title_height - badge_h) / 2
        badge_bottom = badge_top + badge_h
        draw.rectangle([badge_left, badge_top, badge_right, badge_bottom], fill=status_color)
        text_color = COLOR_WHITE if status_color == COLOR_RED else COLOR_BLACK
        draw.text(
            ((badge_left + badge_right) / 2, (badge_top + badge_bottom) / 2),
            status_text,
            font=badge_font,
            fill=text_color,
            anchor="mm",
        )

        if self._poller is not None and self._poller.last_sample_ts:
            updated_dt = datetime.fromtimestamp(self._poller.last_sample_ts, now_dt.tzinfo)
            updated_str = f"Updated {updated_dt.strftime('%I:%M:%S %p').lstrip('0')}"
            draw.text((badge_left - pad, title_height / 2), updated_str, font=fonts["small"], fill=COLOR_BLACK, anchor="rm")

    def _draw_sparklines(self, draw, box, now_ts, tz, fonts, min_outage_seconds, time_format):
        x0, y0, x1, y1 = box
        gap = round((y1 - y0) * 0.03)
        panel_h = (y1 - y0 - 2 * gap) / 3
        windows = [
            ("Last Hour", 3600),
            ("Last 6 Hours", 6 * 3600),
            ("Last 12 Hours", 12 * 3600),
        ]
        for i, (label, span) in enumerate(windows):
            top = y0 + i * (panel_h + gap)
            bottom = top + panel_h
            since_ts = now_ts - span
            samples = self._store.get_samples(since_ts, now_ts)
            outages = self._filter_outages(self._store.get_outages(since_ts, now_ts, now_ts), min_outage_seconds)
            self._draw_sparkline_panel(draw, (x0, top, x1, bottom), label, since_ts, now_ts, samples, outages, fonts, tz, time_format)

    def _format_clock_time(self, dt, time_format):
        if time_format == "24h":
            return dt.strftime("%H:%M")
        return dt.strftime("%I:%M %p").lstrip("0")

    def _draw_sparkline_panel(self, draw, box, label, since_ts, until_ts, samples, outages, fonts, tz, time_format):
        x0, y0, x1, y1 = box
        pad = 8
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)

        chart_top = y0 + fonts["label"].size + pad * 3
        chart_bottom = y1 - pad * 2 - fonts["small"].size - 6
        chart_left = x0 + pad + 44
        chart_right = x1 - pad

        span = max(1, until_ts - since_ts)

        def x_of(ts):
            ts = min(max(ts, since_ts), until_ts)
            return chart_left + (ts - since_ts) / span * (chart_right - chart_left)

        valid = [(ts, rtt) for ts, rtt in samples if rtt is not None]

        # At 1Hz, an hours-long window has many more samples than the chart has
        # pixel columns, which draws as dense noise. Average into per-pixel-ish
        # buckets first; buckets stay empty across outages/gaps (those rtts are
        # already excluded from `valid`), so the line still breaks there naturally.
        chart_width_px = max(1.0, chart_right - chart_left)
        target_points = max(20, int(chart_width_px // 2))
        bucket_span = max(1.0, span / target_points)

        buckets = {}
        for ts, rtt in valid:
            idx = int((ts - since_ts) / bucket_span)
            bucket_ts, rtts = buckets.get(idx, (ts, []))
            rtts.append(rtt)
            buckets[idx] = (bucket_ts, rtts)

        points = [(bucket_ts, sum(rtts) / len(rtts)) for bucket_ts, rtts in buckets.values()]
        points.sort(key=lambda p: p[0])

        # Scale is the max of what's actually drawn (the bucket-averaged line below),
        # not the raw per-second samples - otherwise a single spiky sample (e.g. one
        # slow TCP handshake) can inflate the axis well above the line's visible peak.
        max_rtt = max((rtt for _, rtt in points), default=0)
        y_max = max_rtt if max_rtt > 0 else 1.0

        def y_of(rtt):
            rtt = min(rtt, y_max)
            return chart_bottom - (rtt / y_max) * (chart_bottom - chart_top)

        # Outage periods (poller alive, pings failing) get a red background band.
        # Periods where the poller wasn't running at all (reboot/restart) are left
        # unshaded - they're already visually distinct since no line is drawn there.
        for start, end in outages:
            seg_start, seg_end = max(start, since_ts), min(end, until_ts)
            if seg_end > seg_start:
                draw.rectangle([x_of(seg_start), chart_top, x_of(seg_end), chart_bottom], fill=COLOR_RED)

        # horizontal reference gridlines at 25/50/75% of the scale, with the
        # midpoint labeled so the scale is readable without staring at just the ends
        for frac in (0.25, 0.5, 0.75):
            gy = chart_bottom - frac * (chart_bottom - chart_top)
            draw.line([(chart_left, gy), (chart_right, gy)], fill=COLOR_BLACK, width=1)
        mid_y = chart_bottom - 0.5 * (chart_bottom - chart_top)
        draw.text((x0 + pad, mid_y), f"{int(y_max / 2)}ms", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)

        prev = None
        for ts, rtt in points:
            if prev is not None and ts - prev[0] <= bucket_span * 2.5:
                draw.line([(x_of(prev[0]), y_of(prev[1])), (x_of(ts), y_of(rtt))], fill=COLOR_BLUE, width=6)
            prev = (ts, rtt)

        draw.text((x0 + pad, y0 + pad), label, font=fonts["label"], fill=COLOR_BLACK, anchor="la")
        draw.text((x0 + pad, chart_top), f"{int(y_max)}ms", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, chart_bottom), "0ms", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        # x-axis time ticks, evenly spaced from the start of the window to now
        tick_y = chart_bottom + 4
        for frac, h_anchor in ((0.0, "l"), (0.25, "m"), (0.5, "m"), (0.75, "m"), (1.0, "r")):
            tick_ts = since_ts + frac * span
            tick_x = x_of(tick_ts)
            draw.line([(tick_x, chart_bottom), (tick_x, chart_bottom + 3)], fill=COLOR_BLACK, width=1)
            tick_dt = datetime.fromtimestamp(tick_ts, tz)
            draw.text((tick_x, tick_y), self._format_clock_time(tick_dt, time_format), font=fonts["small"], fill=COLOR_BLACK, anchor=h_anchor + "a")

    def _draw_histogram(self, draw, box, now_ts, tz, fonts, min_outage_seconds, history_weeks, week_start, aggregation):
        x0, y0, x1, y1 = box
        pad = 10
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)

        # Fixed weekday columns (like a normal calendar), not a rolling window
        # ending on whatever today happens to be - each column is that weekday's
        # outage pattern, aggregated across `history_weeks` weeks if more than one.
        if history_weeks > 1:
            agg_label = {"max": "Max", "min": "Min", "average": "Avg"}[aggregation]
            title = f"Outages Per Hour ({agg_label} over {history_weeks} Weeks)"
        else:
            title = "Outages Per Hour (This Week)"
        draw.text((x0 + pad, y0 + pad), title, font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        chart_top = y0 + fonts["label"].size + pad * 3
        chart_bottom = y1 - pad - fonts["small"].size - 6
        chart_left = x0 + pad + 30
        chart_right = x1 - pad

        today = datetime.fromtimestamp(now_ts, tz).date()
        days_since_week_start = today.weekday() if week_start == "monday" else (today.weekday() + 1) % 7
        this_week_start_date = today - timedelta(days=days_since_week_start)

        oldest_week_start_date = this_week_start_date - timedelta(days=7 * (history_weeks - 1))
        window_start_ts = int(tz.localize(datetime.combine(oldest_week_start_date, datetime.min.time())).timestamp())
        all_outages = self._filter_outages(self._store.get_outages(window_start_ts, now_ts, now_ts), min_outage_seconds)

        # slot_lists[weekday_col][hour] collects one observed count per week that
        # slot has already happened in, so weeks are compared like-for-like by
        # weekday/hour instead of by absolute date; a still-future hour in the
        # current (partial) week is skipped rather than counted as a zero.
        slot_lists = [[[] for _ in range(24)] for _ in range(7)]
        for week_offset in range(history_weeks):
            week_start_date = this_week_start_date - timedelta(days=7 * week_offset)
            week_start_ts = int(tz.localize(datetime.combine(week_start_date, datetime.min.time())).timestamp())
            week_end_ts = week_start_ts + 7 * 86400

            week_counts = [[0] * 24 for _ in range(7)]
            for start, _end in all_outages:
                if week_start_ts <= start < week_end_ts:
                    col, hour = divmod(int((start - week_start_ts) // 3600), 24)
                    if 0 <= col < 7:
                        week_counts[col][hour] += 1

            for col in range(7):
                for hour in range(24):
                    if week_start_ts + col * 86400 + hour * 3600 <= now_ts:
                        slot_lists[col][hour].append(week_counts[col][hour])

        def aggregate(values):
            if not values:
                return 0.0
            if aggregation == "max":
                return float(max(values))
            if aggregation == "min":
                return float(min(values))
            return sum(values) / len(values)

        values = [[aggregate(slot_lists[col][hour]) for hour in range(24)] for col in range(7)]
        max_value = max((v for col in values for v in col), default=0.0) or 1.0

        bucket_count = 7 * 24
        bar_area_w = chart_right - chart_left
        bar_w = bar_area_w / bucket_count

        for col in range(7):
            for hour in range(24):
                value = values[col][hour]
                if value <= 0:
                    continue
                idx = col * 24 + hour
                bx0 = chart_left + idx * bar_w
                bx1 = bx0 + max(bar_w, 1.0)
                bar_h = (value / max_value) * (chart_bottom - chart_top)
                draw.rectangle([bx0, chart_bottom - bar_h, bx1, chart_bottom], fill=COLOR_RED)

        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)
        max_value_str = f"{max_value:.1f}" if aggregation == "average" else str(int(max_value))
        draw.text((x0 + pad, chart_top), max_value_str, font=fonts["small"], fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, chart_bottom), "0", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        weekday_names = (
            ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            if week_start == "monday"
            else ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        )
        for day in range(8):
            dx = chart_left + day * 24 * bar_w
            draw.line([(dx, chart_top), (dx, chart_bottom)], fill=COLOR_BLACK, width=1)
        for day in range(7):
            dx = chart_left + day * 24 * bar_w
            draw.text((dx + 2, chart_bottom + 4), weekday_names[day], font=fonts["small"], fill=COLOR_BLACK, anchor="la")

    def _draw_calendar(self, draw, box, now_ts, tz, fonts, min_outage_seconds):
        x0, y0, x1, y1 = box
        pad = 10
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), "Outages By Day (4 Weeks)", font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        header_h = fonts["small"].size + 6
        grid_top = y0 + fonts["label"].size + pad * 3 + header_h
        grid_bottom = y1 - pad
        grid_left = x0 + pad
        grid_right = x1 - pad

        today = datetime.fromtimestamp(now_ts, tz).date()
        start_date = today - timedelta(days=27)
        cols, rows = 7, 4
        cell_w = (grid_right - grid_left) / cols
        cell_h = (grid_bottom - grid_top) / rows

        first_started_ts = self._store.get_meta_int("first_started_ts", now_ts)
        window_start_ts = int(tz.localize(datetime.combine(start_date, datetime.min.time())).timestamp())
        all_outages = self._filter_outages(self._store.get_outages(window_start_ts, now_ts, now_ts), min_outage_seconds)
        all_gaps = self._store.get_gaps(window_start_ts, now_ts)

        for c in range(cols):
            wd_date = start_date + timedelta(days=c)
            draw.text(
                (grid_left + c * cell_w + cell_w / 2, grid_top - header_h / 2),
                wd_date.strftime("%a"),
                font=fonts["small"],
                fill=COLOR_BLACK,
                anchor="mm",
            )

        for i in range(28):
            day_date = start_date + timedelta(days=i)
            row, col = divmod(i, cols)
            cx0 = grid_left + col * cell_w
            cy0 = grid_top + row * cell_h
            cx1 = cx0 + cell_w
            cy1 = cy0 + cell_h

            day_start_ts = int(tz.localize(datetime.combine(day_date, datetime.min.time())).timestamp())
            day_end_ts = day_start_ts + 86400

            fill = None
            if day_start_ts <= now_ts and day_end_ts > first_started_ts:
                gap_secs = self._overlap_seconds(all_gaps, day_start_ts, day_end_ts)
                if gap_secs < 23 * 3600:
                    count = sum(1 for start, _end in all_outages if day_start_ts <= start < day_end_ts)
                    if count == 0:
                        fill = COLOR_GREEN
                    elif count <= 2:
                        fill = COLOR_YELLOW
                    else:
                        fill = COLOR_RED

            draw.rectangle([cx0, cy0, cx1, cy1], fill=fill or COLOR_WHITE, outline=COLOR_BLACK, width=1)
            if fill is None:
                self._draw_hatch(draw, (cx0, cy0, cx1, cy1))
            if day_date == today:
                draw.rectangle([cx0, cy0, cx1, cy1], outline=COLOR_BLACK, width=3)

            text_color = COLOR_WHITE if fill == COLOR_RED else COLOR_BLACK
            draw.text((cx0 + 4, cy0 + 2), str(day_date.day), font=fonts["small"], fill=text_color, anchor="la")

    @staticmethod
    def _overlap_seconds(episodes, window_start, window_end):
        total = 0
        for start, end in episodes:
            seg_start, seg_end = max(start, window_start), min(end, window_end)
            if seg_end > seg_start:
                total += seg_end - seg_start
        return total

    @staticmethod
    def _draw_hatch(draw, box, spacing=10):
        x0, y0, x1, y1 = box
        x0i, y0i, x1i, y1i = round(x0), round(y0), round(x1), round(y1)
        offset = x1i - x0i + y1i - y0i
        for d in range(0, offset, spacing):
            x_start, y_start = x0i + d, y0i
            if x_start > x1i:
                y_start += x_start - x1i
                x_start = x1i
            x_end, y_end = x0i, y0i + d
            if y_end > y1i:
                x_end += y_end - y1i
                y_end = y1i
            draw.line([(x_start, y_start), (x_end, y_end)], fill=COLOR_BLACK, width=1)
