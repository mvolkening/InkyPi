#!/usr/bin/env python3
"""Internet connectivity + speed monitor, meant to run 24/7 on a hard-wired Pi.

Two jobs, both writing to a local InfluxDB v2 (stdlib only, no pip installs):

  1. Ping: probes a failover chain of public resolvers once a second and records
     every sample, plus derived outage / polling-gap episodes.
  2. Speed test: runs `speedtest-cli` on an adjustable schedule and records
     download / upload throughput.

The InkyPi "Internet Outage Monitor" plugin reads all of this back out of
InfluxDB and just draws it, so history survives InkyPi restarts and the ping
vantage point is the wired Pi instead of a Wi-Fi display.

Data model
----------
raw bucket (short retention, 1 point per second):
    ping        fields: up (int 0/1), rtt_ms (float, only when up), host (str, only when up)

main bucket (long retention, tiny):
    outage      time = outage start; fields: end_ts (int epoch s, 0 = still ongoing),
                scope (str: isp | local | unknown)
    gap         time = last sample before the daemon stopped; fields: end_ts (int)
    speedtest   fields: download_mbps, upload_mbps, idle_latency_ms, server (str)
    meta        field: first_started_ts (int), written once

Configuration is re-read from the config file whenever it changes, so the ping
hosts and speed-test schedule can be adjusted without restarting the service.
"""

import argparse
import configparser
import csv
import io
import json
import logging
import os
import shlex
import signal
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque

logger = logging.getLogger("netmon")

DEFAULT_CONFIG_PATH = "/etc/netmon/netmon.conf"
DEFAULT_STATE_PATH = "/var/lib/netmon/state.json"

DEFAULTS = {
    "influx": {
        "url": "http://localhost:8086",
        "org": "none",
        "token": "",
        "raw_bucket": "netmon_raw",
        "bucket": "netmon",
    },
    "ping": {
        "interval_seconds": "1",
        "timeout_seconds": "1",
        "hosts": "1.1.1.1, 8.8.8.8, 9.9.9.9",
        "gateway": "",  # blank = auto-detect
    },
    "speedtest": {
        "enabled": "true",
        "interval_minutes": "60",
        "timeout_seconds": "180",
        "command": "speedtest-cli --json --secure",
        "retry_minutes": "10",  # wait before retrying after a failed/skipped test
    },
}

TCP_FALLBACK_PORT = 53
GATEWAY_TCP_PORT = 80
FLUSH_INTERVAL_SECONDS = 5
# If InfluxDB is unreachable, keep at most this many unsent samples (~1h at 1Hz)
# rather than growing without bound.
MAX_BUFFERED_SAMPLES = 3600
# Restart/reboot detection: a bigger gap than the normal tick-to-tick spacing.
STARTUP_GAP_THRESHOLD_SECONDS = 5
# Ping samples older than this mean the daemon isn't running; used for recovery.
IDLE_LATENCY_SAMPLES = 60


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

class Config:
    """Config file that transparently reloads when its mtime changes."""

    def __init__(self, path):
        self.path = path
        self._mtime = None
        self._parser = self._new_parser()
        self._lock = threading.Lock()
        self.reload_if_changed()

    @staticmethod
    def _new_parser():
        parser = configparser.ConfigParser(interpolation=None)
        parser.read_dict(DEFAULTS)
        return parser

    def reload_if_changed(self):
        try:
            mtime = os.stat(self.path).st_mtime
        except OSError:
            if self._mtime is None:
                logger.warning("Config file %s not found; using defaults.", self.path)
                self._mtime = 0
            return
        if mtime == self._mtime:
            return
        parser = self._new_parser()
        try:
            parser.read(self.path)
        except configparser.Error:
            logger.exception("Could not parse %s; keeping the previous settings.", self.path)
            return
        with self._lock:
            self._parser = parser
            first_load = self._mtime is None
            self._mtime = mtime
        if not first_load:
            logger.info("Reloaded configuration from %s.", self.path)

    def get(self, section, key):
        with self._lock:
            return self._parser.get(section, key)

    def get_float(self, section, key, minimum=None):
        try:
            value = float(self.get(section, key))
        except ValueError:
            value = float(DEFAULTS[section][key])
        return max(minimum, value) if minimum is not None else value

    def get_bool(self, section, key):
        return self.get(section, key).strip().lower() in ("1", "true", "yes", "on")

    def hosts(self):
        hosts = [h.strip() for h in self.get("ping", "hosts").split(",") if h.strip()]
        return hosts or [h.strip() for h in DEFAULTS["ping"]["hosts"].split(",")]


# ---------------------------------------------------------------------------
# InfluxDB (v2 HTTP API)
# ---------------------------------------------------------------------------

class InfluxError(RuntimeError):
    pass


def _escape_str_field(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _flux_str(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


class Influx:
    def __init__(self, config):
        self.config = config

    def _request(self, path, params, data, headers):
        url = self.config.get("influx", "url").rstrip("/") + path + "?" + urllib.parse.urlencode(params)
        headers = dict(headers, Authorization="Token " + self.config.get("influx", "token"))
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raise InfluxError(f"HTTP {e.code} from {path}: {e.read().decode('utf-8', 'replace')[:200]}")
        except (urllib.error.URLError, OSError) as e:
            raise InfluxError(f"Could not reach InfluxDB: {e}")

    def write(self, bucket_key, lines):
        """Writes line-protocol lines (timestamps in epoch seconds) to a bucket
        named by a key in the [influx] section (raw_bucket / bucket)."""
        if not lines:
            return
        params = {"org": self.config.get("influx", "org"), "bucket": self.config.get("influx", bucket_key), "precision": "s"}
        self._request("/api/v2/write", params, "\n".join(lines).encode("utf-8"), {"Content-Type": "text/plain; charset=utf-8"})

    def query(self, flux):
        params = {"org": self.config.get("influx", "org")}
        text = self._request(
            "/api/v2/query", params, flux.encode("utf-8"),
            {"Content-Type": "application/vnd.flux", "Accept": "application/csv"},
        )
        rows, header = [], None
        for row in csv.reader(io.StringIO(text)):
            if not row or row == [""]:
                header = None
                continue
            if row[0].startswith("#"):
                continue
            if header is None:
                header = row
                continue
            rows.append(dict(zip(header, row)))
        return rows

    def bucket(self, key):
        return _flux_str(self.config.get("influx", key))


def _parse_rfc3339(value):
    from datetime import datetime, timezone
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return int(datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# probing
# ---------------------------------------------------------------------------

def _icmp_checksum(data):
    if len(data) % 2:
        data += b"\x00"
    total = sum(struct.unpack("!%dH" % (len(data) // 2), data))
    total = (total >> 16) + (total & 0xFFFF)
    total += total >> 16
    return ~total & 0xFFFF


def _detect_default_gateway():
    try:
        with open("/proc/net/route") as f:
            for line in f.readlines()[1:]:
                fields = line.split()
                if len(fields) >= 3 and fields[1] == "00000000":
                    return socket.inet_ntoa(struct.pack("<L", int(fields[2], 16)))
    except Exception:
        logger.debug("Could not read default gateway", exc_info=True)
    return None


class Prober:
    """RTT probes: raw ICMP (CAP_NET_RAW), then unprivileged ICMP datagram
    sockets, then TCP-connect timing as a last resort."""

    def __init__(self):
        self._mode = None  # None = untested, else 'raw' | 'dgram' | 'tcp'
        self._seq = 0

    def probe(self, host, timeout):
        """Returns RTT in ms, or None if the host didn't answer in time."""
        if self._mode in (None, "raw", "dgram"):
            for mode, sock_type in (("raw", socket.SOCK_RAW), ("dgram", socket.SOCK_DGRAM)):
                if self._mode not in (None, mode):
                    continue
                result = self._icmp(host, timeout, mode, sock_type)
                if result is not NotImplemented:
                    if self._mode is None:
                        logger.info("Using %s ICMP sockets for pings.", mode)
                    self._mode = mode
                    return result
            self._mode = "tcp"
            logger.info("ICMP sockets unavailable (needs CAP_NET_RAW); falling back to TCP connect timing.")
        return self._tcp(host, timeout)

    def _icmp(self, host, timeout, mode, sock_type):
        try:
            sock = socket.socket(socket.AF_INET, sock_type, socket.IPPROTO_ICMP)
        except OSError:
            return NotImplemented
        try:
            self._seq = (self._seq + 1) & 0xFFFF
            ident = os.getpid() & 0xFFFF
            payload = struct.pack("d", time.time())
            header = struct.pack("!BBHHH", 8, 0, 0, ident, self._seq)
            checksum = _icmp_checksum(header + payload)
            packet = struct.pack("!BBHHH", 8, 0, checksum, ident, self._seq) + payload

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
                except OSError:  # includes socket.timeout
                    return None
                recv_time = time.time()
                if addr[0] != host:
                    continue
                # Raw sockets deliver the IP header too; datagram ICMP sockets don't.
                offset = (reply[0] & 0x0F) * 4 if mode == "raw" and reply else 0
                icmp = reply[offset:offset + 8]
                if len(icmp) < 8:
                    continue
                reply_type, _code, _csum, reply_id, reply_seq = struct.unpack("!BBHHH", icmp)
                # A datagram ICMP socket rewrites the id to its own port, so only
                # the sequence number is meaningful there.
                id_ok = reply_id == ident if mode == "raw" else True
                if reply_type == 0 and id_ok and reply_seq == self._seq:
                    return (recv_time - send_time) * 1000.0
        finally:
            sock.close()

    @staticmethod
    def _tcp(host, timeout, port=TCP_FALLBACK_PORT):
        start = time.time()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return (time.time() - start) * 1000.0
        except OSError:
            return None

    def gateway_reachable(self, host, timeout):
        """Reachability only; a TCP connection being actively refused still means
        something at that address answered."""
        if self.probe(host, timeout) is not None:
            return True
        try:
            with socket.create_connection((host, GATEWAY_TCP_PORT), timeout=timeout):
                return True
        except ConnectionRefusedError:
            return True
        except OSError:
            return False


# ---------------------------------------------------------------------------
# state shared between the ping and speed-test threads
# ---------------------------------------------------------------------------

class SharedState:
    def __init__(self, state_path):
        self.state_path = state_path
        self.in_outage = False
        self.recent_rtts = deque(maxlen=IDLE_LATENCY_SAMPLES)
        self._lock = threading.Lock()

    def idle_latency_ms(self):
        with self._lock:
            values = list(self.recent_rtts)
        return statistics.median(values) if values else None

    def add_rtt(self, rtt):
        with self._lock:
            self.recent_rtts.append(rtt)

    def load(self):
        try:
            with open(self.state_path) as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def save(self, **updates):
        state = self.load()
        state.update(updates)
        try:
            os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
            tmp = self.state_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump(state, f)
            os.replace(tmp, self.state_path)
        except OSError:
            logger.exception("Could not save state to %s", self.state_path)


# ---------------------------------------------------------------------------
# ping thread
# ---------------------------------------------------------------------------

class PingMonitor(threading.Thread):
    def __init__(self, config, influx, shared, stop_event):
        super().__init__(name="ping", daemon=True)
        self.config = config
        self.influx = influx
        self.shared = shared
        self.stop_event = stop_event
        self.prober = Prober()
        self._gateway = None
        self._gateway_checked = 0.0
        self._pending_raw = []       # unsent ping lines
        self._pending_episodes = []  # unsent outage/gap lines (never dropped)

    # -- startup recovery ----------------------------------------------------

    def _recover(self):
        """Runs once at startup: records the polling gap since the last sample
        (restart / reboot) and closes any outage left open by a previous run, and
        stamps the first-ever start time."""
        now = int(time.time())
        raw = self.influx.bucket("raw_bucket")
        main = self.influx.bucket("bucket")

        rows = self.influx.query(
            f'from(bucket: "{raw}") |> range(start: -48h) '
            '|> filter(fn: (r) => r._measurement == "ping" and r._field == "up") |> last()'
        )
        last_ts = _parse_rfc3339(rows[0]["_time"]) if rows else None

        lines = []
        if last_ts is not None and now - last_ts > STARTUP_GAP_THRESHOLD_SECONDS:
            lines.append(f"gap end_ts={now}i {last_ts}")
            logger.info("Recorded a %ds polling gap (restart/reboot).", now - last_ts)

        # An outage still marked ongoing was cut short by the previous run ending;
        # close it at the last moment we knew about rather than leaving it open forever.
        open_outages = self.influx.query(
            f'from(bucket: "{main}") |> range(start: -400d) '
            '|> filter(fn: (r) => r._measurement == "outage" and r._field == "end_ts" and r._value == 0)'
        )
        for row in open_outages:
            start = _parse_rfc3339(row["_time"])
            if start is not None:
                lines.append(f"outage end_ts={max(last_ts or start, start)}i {start}")

        if not self.influx.query(
            f'from(bucket: "{main}") |> range(start: -400d) '
            '|> filter(fn: (r) => r._measurement == "meta" and r._field == "first_started_ts") |> last()'
        ):
            lines.append(f"meta first_started_ts={now}i {now}")

        self.influx.write("bucket", lines)

    # -- main loop -----------------------------------------------------------

    def run(self):
        while not self.stop_event.is_set():
            try:
                self._recover()
                break
            except InfluxError as e:
                logger.warning("Startup recovery deferred, InfluxDB not ready: %s", e)
                self.stop_event.wait(10)
            except Exception:
                logger.exception("Startup recovery failed; continuing without it.")
                break

        outage_start = None
        last_flush = time.monotonic()
        interval = 1.0

        while not self.stop_event.is_set():
            loop_start = time.monotonic()
            try:
                self.config.reload_if_changed()
                interval = self.config.get_float("ping", "interval_seconds", minimum=0.2)
                timeout = self.config.get_float("ping", "timeout_seconds", minimum=0.2)

                rtt, host, scope = self._ping_once(timeout)
                ts = int(time.time())

                if rtt is not None:
                    self.shared.add_rtt(rtt)
                    self._pending_raw.append(f'ping up=1i,rtt_ms={rtt:.3f},host="{_escape_str_field(host)}" {ts}')
                else:
                    self._pending_raw.append(f"ping up=0i {ts}")

                if rtt is None and outage_start is None:
                    outage_start = ts
                    self.shared.in_outage = True
                    logger.warning("Internet outage detected (scope=%s).", scope)
                    self._pending_episodes.append(f'outage end_ts=0i,scope="{scope}" {ts}')
                elif rtt is not None and outage_start is not None:
                    logger.warning("Internet outage resolved after %ds (answered by %s).", ts - outage_start, host)
                    self._pending_episodes.append(f"outage end_ts={ts}i {outage_start}")
                    outage_start = None
                    self.shared.in_outage = False

                if time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS or self._pending_episodes:
                    self._flush()
                    last_flush = time.monotonic()
            except Exception:
                logger.exception("Unexpected error in ping loop; continuing")

            self.stop_event.wait(max(0.0, interval - (time.monotonic() - loop_start)))

        try:
            self._flush()
        except Exception:
            logger.debug("Final flush failed", exc_info=True)

    def _flush(self):
        try:
            # Episodes first: they're the important, low-volume data.
            if self._pending_episodes:
                self.influx.write("bucket", self._pending_episodes)
                self._pending_episodes = []
            if self._pending_raw:
                self.influx.write("raw_bucket", self._pending_raw)
                self._pending_raw = []
        except InfluxError as e:
            logger.warning("InfluxDB write failed (will retry): %s", e)
            if len(self._pending_raw) > MAX_BUFFERED_SAMPLES:
                del self._pending_raw[: len(self._pending_raw) - MAX_BUFFERED_SAMPLES]

    def _ping_once(self, timeout):
        """Returns (rtt_ms, host, scope). First host to answer wins; only if every
        host fails is the local gateway checked, to tell a local network problem
        apart from an ISP/backbone outage."""
        for host in self.config.hosts():
            rtt = self.prober.probe(host, timeout)
            if rtt is not None:
                return rtt, host, None
        return None, None, self._outage_scope(timeout)

    def _outage_scope(self, timeout):
        gateway = self.config.get("ping", "gateway").strip() or self._auto_gateway()
        if not gateway:
            return "unknown"
        return "isp" if self.prober.gateway_reachable(gateway, timeout) else "local"

    def _auto_gateway(self):
        now = time.monotonic()
        if self._gateway is None or now - self._gateway_checked > 300:
            self._gateway = _detect_default_gateway()
            self._gateway_checked = now
        return self._gateway


# ---------------------------------------------------------------------------
# speed test thread
# ---------------------------------------------------------------------------

class SpeedTester(threading.Thread):
    def __init__(self, config, influx, shared, stop_event):
        super().__init__(name="speedtest", daemon=True)
        self.config = config
        self.influx = influx
        self.shared = shared
        self.stop_event = stop_event

    def run(self):
        while not self.stop_event.is_set():
            try:
                delay = self._tick()
            except Exception:
                logger.exception("Unexpected error in speed test loop; continuing")
                delay = 60
            # Wake up at least every 30s so interval/enable changes take effect quickly.
            self.stop_event.wait(min(delay, 30))

    def _tick(self):
        """Returns seconds until this should be evaluated again."""
        self.config.reload_if_changed()
        if not self.config.get_bool("speedtest", "enabled"):
            return 30

        state = self.shared.load()
        now = time.time()
        interval = self.config.get_float("speedtest", "interval_minutes", minimum=1) * 60
        retry = self.config.get_float("speedtest", "retry_minutes", minimum=1) * 60

        # Normally due one interval after the last success. If the last attempt
        # failed (or was skipped for an outage), back off by the shorter retry
        # delay from that attempt instead of hammering.
        last_success, last_attempt = state.get("last_success", 0), state.get("last_attempt", 0)
        due_at = last_attempt + retry if last_attempt > last_success else last_success + interval
        if now < due_at:
            return due_at - now

        if self.shared.in_outage:
            logger.info("Skipping speed test: internet is currently down.")
            self.shared.save(last_attempt=now)
            return 30

        self.shared.save(last_attempt=now)
        if self._run_test() is not None:
            self.shared.save(last_success=time.time())
        return 30

    def _run_test(self):
        command = shlex.split(self.config.get("speedtest", "command"))
        timeout = self.config.get_float("speedtest", "timeout_seconds", minimum=30)
        # Measured before the test starts: the test itself saturates the link and
        # would make the "current" ping look terrible.
        idle_latency = self.shared.idle_latency_ms()

        logger.info("Running speed test: %s", " ".join(command))
        started = time.time()
        try:
            completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.warning("Speed test timed out after %.0fs.", timeout)
            return None
        except OSError as e:
            logger.error("Could not run %s: %s", command[0], e)
            return None
        if completed.returncode != 0:
            logger.warning("Speed test failed (exit %d): %s", completed.returncode, completed.stderr.strip()[-300:])
            return None

        try:
            data = json.loads(completed.stdout)
            download_mbps = float(data["download"]) / 1e6
            upload_mbps = float(data["upload"]) / 1e6
        except (ValueError, KeyError, TypeError):
            logger.warning("Could not parse speed test output: %r", completed.stdout[:200])
            return None

        server = (data.get("server") or {})
        server_name = ", ".join(p for p in (server.get("sponsor"), server.get("name")) if p)
        fields = [f"download_mbps={download_mbps:.2f}", f"upload_mbps={upload_mbps:.2f}"]
        if idle_latency is not None:
            fields.append(f"idle_latency_ms={idle_latency:.2f}")
        if server_name:
            fields.append(f'server="{_escape_str_field(server_name)}"')

        try:
            self.influx.write("bucket", [f"speedtest {','.join(fields)} {int(started)}"])
        except InfluxError as e:
            logger.warning("Speed test succeeded but could not be stored: %s", e)
            return None
        logger.info("Speed test: %.1f Mbps down, %.1f Mbps up (%s).", download_mbps, upload_mbps, server_name or "unknown server")
        return download_mbps, upload_mbps


# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--config", default=os.environ.get("NETMON_CONFIG", DEFAULT_CONFIG_PATH))
    parser.add_argument("--state", default=os.environ.get("NETMON_STATE", DEFAULT_STATE_PATH))
    parser.add_argument("--speedtest-now", action="store_true", help="run one speed test, store it, and exit")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")

    config = Config(args.config)
    influx = Influx(config)
    shared = SharedState(args.state)
    stop_event = threading.Event()

    if args.speedtest_now:
        return 0 if SpeedTester(config, influx, shared, stop_event)._run_test() else 1

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_event.set())

    workers = [PingMonitor(config, influx, shared, stop_event), SpeedTester(config, influx, shared, stop_event)]
    for worker in workers:
        worker.start()
    logger.info("netmon started (config: %s).", args.config)

    while not stop_event.wait(1):
        if not all(w.is_alive() for w in workers):
            logger.error("A worker thread died; exiting so systemd restarts the service.")
            return 1
    for worker in workers:
        worker.join(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
