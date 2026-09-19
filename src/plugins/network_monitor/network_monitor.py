import csv
import io
import logging
import re
import time
from datetime import datetime, timedelta

import pytz
import requests
from PIL import Image, ImageDraw

from plugins.base_plugin.base_plugin import BasePlugin
from utils.app_utils import get_font

logger = logging.getLogger(__name__)

# Pinging and speed tests run on a separate, hard-wired Pi (see
# scripts/pidisplay_netmon/) and are stored in its InfluxDB; this plugin only
# reads that data back and draws it.
DEFAULT_INFLUX_URL = "http://PiDisplay.home.local:8086"
DEFAULT_INFLUX_ORG = "none"
DEFAULT_RAW_BUCKET = "netmon_raw"
DEFAULT_BUCKET = "netmon"
SPEEDTEST_WINDOW_HOURS = 48
# Speed graph's fixed y-axis top, matched to the subscribed plan so a glance shows
# how close to the plan speed the link is; override per instance with `speedMaxMbps`.
DEFAULT_SPEED_MAX_MBPS = 500

# (connect, read) timeouts for the LAN-hosted, Pi-backed InfluxDB.
REQUEST_TIMEOUT = (5, 30)

# The daemon writes a ping sample every second; if the newest one is older than
# this, the daemon (or its Pi) isn't running and the live status can't be trusted.
STALE_SAMPLE_SECONDS = 30
STATUS_LOOKBACK = "-1h"
# Episode data (outages/gaps) is tiny, so it's fetched in one query covering the
# calendar's 4 weeks plus the histogram's longest configurable history.
EPISODE_LOOKBACK_DAYS = 100
MAX_HISTORY_WEEKS = 12

# Fixed 6-color e-ink palette - all fills below are flat colors from this set,
# deliberately with no gradients/alpha blending, which don't survive e-ink dithering.
COLOR_BLACK = "#000000"
COLOR_WHITE = "#ffffff"
COLOR_RED = "#a6242b"
COLOR_YELLOW = "#dfaf2c"
COLOR_BLUE = "#345f94"
COLOR_GREEN = "#428c46"

DEFAULT_TITLE = "Internet Outage Monitor"


class InfluxQueryError(RuntimeError):
    pass


def _flux_escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _parse_annotated_csv(text):
    """Parses InfluxDB's "annotated CSV" query response into a list of row dicts,
    skipping '#'-prefixed annotation lines and resetting the header on each blank
    line (InfluxDB emits one header per result table)."""
    rows = []
    header = None
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


def _parse_influx_time(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return int(datetime.strptime(value, fmt).replace(tzinfo=pytz.UTC).timestamp())
        except ValueError:
            continue
    return None


def _parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_rfc3339(ts):
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


class _InfluxStore:
    """Read-only view of the ping/outage/speed-test data the daemon writes to
    InfluxDB, exposing the small query surface the drawing code needs. `load()`
    prefetches live status and the low-volume episode data once per render so the
    histogram, calendar and sparklines don't each re-query it."""

    def __init__(self, url, org, token, raw_bucket, bucket, verify_ssl):
        self.url = url.rstrip("/")
        self.org = org
        self.token = token
        self.raw_bucket = raw_bucket
        self.bucket = bucket
        self.verify_ssl = verify_ssl
        self._outages = []  # [(start_ts, end_ts, or None while still ongoing)]
        self._gaps = []
        self._first_started_ts = None
        self.last_sample_ts = None
        self.last_up = None
        self.outage_scope = None

    def _query(self, flux):
        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        }
        # Org IDs are unambiguous 16-char hex strings; org names must match exactly.
        org_param = {"orgID": self.org} if re.fullmatch(r"[0-9a-fA-F]{16}", self.org) else {"org": self.org}
        try:
            response = requests.post(
                f"{self.url}/api/v2/query", params=org_param, headers=headers,
                data=flux.encode("utf-8"), timeout=REQUEST_TIMEOUT, verify=self.verify_ssl,
            )
        except requests.RequestException as e:
            raise InfluxQueryError(f"Could not reach InfluxDB at {self.url}: {e}")
        if not 200 <= response.status_code < 300:
            raise InfluxQueryError(f"InfluxDB query failed with status {response.status_code}: {response.text[:300]}")
        return _parse_annotated_csv(response.text)

    def _pivoted(self, measurement, start):
        return self._query(
            f'from(bucket: "{_flux_escape(self.bucket)}") |> range(start: {start}) '
            f'|> filter(fn: (r) => r._measurement == "{measurement}") '
            '|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
        )

    def load(self):
        """Fetches live status and all outage/gap episodes."""
        rows = self._query(
            f'from(bucket: "{_flux_escape(self.raw_bucket)}") |> range(start: {STATUS_LOOKBACK}) '
            '|> filter(fn: (r) => r._measurement == "ping" and r._field == "up") |> last()'
        )
        if rows:
            self.last_sample_ts = _parse_influx_time(rows[0].get("_time"))
            self.last_up = _parse_int(rows[0].get("_value")) == 1

        start = f"-{EPISODE_LOOKBACK_DAYS}d"
        self._outages, self.outage_scope = [], None
        for row in self._pivoted("outage", start):
            start_ts, end_ts = _parse_influx_time(row.get("_time")), _parse_int(row.get("end_ts"))
            if start_ts is None or end_ts is None:
                continue
            if end_ts == 0:  # daemon writes 0 while an outage is still open
                end_ts = None
                self.outage_scope = row.get("scope") or None
            self._outages.append((start_ts, end_ts))

        self._gaps = []
        for row in self._pivoted("gap", start):
            gap_start, gap_end = _parse_influx_time(row.get("_time")), _parse_int(row.get("end_ts"))
            if gap_start is not None and gap_end is not None:
                self._gaps.append((gap_start, gap_end))

        rows = self._query(
            f'from(bucket: "{_flux_escape(self.bucket)}") |> range(start: -400d) '
            '|> filter(fn: (r) => r._measurement == "meta" and r._field == "first_started_ts") |> min()'
        )
        self._first_started_ts = _parse_int(rows[0].get("_value")) if rows else None

    def has_any_data(self):
        return self.last_sample_ts is not None

    def get_samples(self, since_ts, until_ts):
        """Mean RTT per time bucket (failed pings carry no rtt_ms, so they just
        leave empty buckets). Aggregated server-side: a 12h window is ~43k raw
        samples, far more than the chart has pixel columns."""
        every = max(1, (until_ts - since_ts) // 700)
        rows = self._query(
            f'from(bucket: "{_flux_escape(self.raw_bucket)}") '
            f'|> range(start: {_to_rfc3339(since_ts)}, stop: {_to_rfc3339(until_ts + 1)}) '
            '|> filter(fn: (r) => r._measurement == "ping" and r._field == "rtt_ms") '
            f'|> aggregateWindow(every: {every}s, fn: mean, createEmpty: false) '
            '|> keep(columns: ["_time", "_value"])'
        )
        samples = []
        for row in rows:
            ts, value = _parse_influx_time(row.get("_time")), _parse_float(row.get("_value"))
            if ts is not None and value is not None:
                samples.append((ts, value))
        samples.sort()
        return samples

    def get_outages(self, since_ts, until_ts, now_ts):
        # An outage still open when the daemon stopped reporting ends at the last
        # sample instead of running on to "now".
        open_end = now_ts
        if self.last_sample_ts is not None and now_ts - self.last_sample_ts > STALE_SAMPLE_SECONDS:
            open_end = self.last_sample_ts
        return [
            (start, end if end is not None else max(start, open_end))
            for start, end in self._outages
            if (end is None or end >= since_ts) and start <= until_ts
        ]

    def get_gaps(self, since_ts, until_ts):
        return [(start, end) for start, end in self._gaps if end >= since_ts and start <= until_ts]

    def get_meta_int(self, key, default):
        if key == "first_started_ts" and self._first_started_ts is not None:
            return self._first_started_ts
        return default

    def get_speedtests(self, since_ts):
        """[(ts, download_mbps, upload_mbps, idle_latency_ms or None, server or None)], oldest first."""
        tests = []
        for row in self._pivoted("speedtest", _to_rfc3339(since_ts)):
            ts = _parse_influx_time(row.get("_time"))
            down, up = _parse_float(row.get("download_mbps")), _parse_float(row.get("upload_mbps"))
            if ts is None or down is None or up is None:
                continue
            tests.append((ts, down, up, _parse_float(row.get("idle_latency_ms")), row.get("server") or None))
        tests.sort()
        return tests


class NetworkMonitor(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params['api_key'] = {
            "required": True,
            "service": "InfluxDB (Network Monitor)",
            "expected_key": "INFLUXDB_NETMON_TOKEN"
        }
        return template_params

    def generate_image(self, settings, device_config):
        influx_url = (settings.get("influxUrl") or "").strip() or DEFAULT_INFLUX_URL
        influx_org = (settings.get("influxOrg") or "").strip() or DEFAULT_INFLUX_ORG
        raw_bucket = (settings.get("rawBucket") or "").strip() or DEFAULT_RAW_BUCKET
        bucket = (settings.get("influxBucket") or "").strip() or DEFAULT_BUCKET
        verify_ssl = str(settings.get("skipTlsVerify", "")).lower() != "true"

        token = device_config.load_env_key("INFLUXDB_NETMON_TOKEN")
        if not token:
            raise RuntimeError("InfluxDB token not configured (set INFLUXDB_NETMON_TOKEN in .env).")

        title = (settings.get("title") or "").strip() or DEFAULT_TITLE

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        timezone_name = device_config.get_config("timezone", default="America/New_York")
        time_format = device_config.get_config("time_format", default="12h")
        tz = pytz.timezone(timezone_name)
        now_dt = datetime.now(tz)
        now_ts = int(now_dt.timestamp())

        store = _InfluxStore(influx_url, influx_org, token, raw_bucket, bucket, verify_ssl)
        try:
            store.load()
            self._store = store
            width, height = dimensions
            fonts = {
                "title": get_font("Jost", round(height * 0.040), "bold"),
                "label": get_font("Jost", round(height * 0.020), "bold"),
                "small": get_font("Jost", round(height * 0.014), "normal"),
            }

            if not store.has_any_data():
                return self._render_waiting_image(dimensions, title, fonts)

            image = Image.new("RGB", dimensions, COLOR_WHITE)
            draw = ImageDraw.Draw(image)

            title_height = round(height * 0.075)
            self._draw_title(draw, width, title_height, title, now_dt, fonts, store, time_format)

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
            self._draw_left_column(draw, left_box, now_ts, tz, fonts, min_outage_seconds, time_format, self._parse_speed_max(settings))
            self._draw_histogram(draw, histogram_box, now_ts, tz, fonts, min_outage_seconds, history_weeks, week_start, aggregation)
            self._draw_calendar(draw, calendar_box, now_ts, tz, fonts, min_outage_seconds)
        except InfluxQueryError as e:
            logger.error(f"Failed to query InfluxDB: {e}")
            raise RuntimeError("Failed to retrieve network monitor data from InfluxDB, please check logs.")

        return image

    @staticmethod
    def _parse_speed_max(settings):
        try:
            value = float(settings.get("speedMaxMbps"))
        except (TypeError, ValueError):
            value = DEFAULT_SPEED_MAX_MBPS
        return value if value > 0 else DEFAULT_SPEED_MAX_MBPS

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
        store = self._store
        if store.last_sample_ts is None or time.time() - store.last_sample_ts > STALE_SAMPLE_SECONDS:
            return "Monitor Offline", COLOR_YELLOW
        if store.last_up:
            return "Online", COLOR_GREEN
        if store.outage_scope == "local":
            return "Outage (Local)", COLOR_RED
        if store.outage_scope == "isp":
            return "Outage (ISP)", COLOR_RED
        return "Outage", COLOR_RED

    def _draw_title(self, draw, width, title_height, title, now_dt, fonts, store, time_format):
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

        if store.last_sample_ts:
            updated_dt = datetime.fromtimestamp(store.last_sample_ts, now_dt.tzinfo)
            updated_str = f"Updated {updated_dt.strftime('%I:%M:%S %p').lstrip('0')}"
            draw.text((badge_left - pad, title_height / 2), updated_str, font=fonts["small"], fill=COLOR_BLACK, anchor="rm")

    def _draw_left_column(self, draw, box, now_ts, tz, fonts, min_outage_seconds, time_format, speed_max_mbps):
        """Ping sparklines for the last hour (top) and 12 hours (bottom), with the
        speed-test history between them."""
        x0, y0, x1, y1 = box
        gap = round((y1 - y0) * 0.03)
        panel_h = (y1 - y0 - 2 * gap) / 3
        panels = [("Last Hour", 3600), ("speed", None), ("Last 12 Hours", 12 * 3600)]
        for i, (label, span) in enumerate(panels):
            top = y0 + i * (panel_h + gap)
            panel_box = (x0, top, x1, top + panel_h)
            if span is None:
                self._draw_speed_panel(draw, panel_box, now_ts, tz, fonts, speed_max_mbps)
                continue
            since_ts = now_ts - span
            samples = self._store.get_samples(since_ts, now_ts)
            outages = self._filter_outages(self._store.get_outages(since_ts, now_ts, now_ts), min_outage_seconds)
            self._draw_sparkline_panel(draw, panel_box, label, since_ts, now_ts, samples, outages, fonts, tz, time_format)

    def _draw_speed_panel(self, draw, box, now_ts, tz, fonts, y_max):
        """Download/upload throughput from the daemon's scheduled speed tests over
        the last SPEEDTEST_WINDOW_HOURS, plus the most recent reading."""
        x0, y0, x1, y1 = box
        pad = 8
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)

        since_ts = now_ts - SPEEDTEST_WINDOW_HOURS * 3600
        tests = self._store.get_speedtests(since_ts)

        draw.text((x0 + pad, y0 + pad), f"Speed Test - Mbps (Last {SPEEDTEST_WINDOW_HOURS} Hours)", font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        # Legend row doubles as the latest reading so the current numbers are visible
        # without reading them off the chart.
        legend_font = fonts["small"]
        swatch = legend_font.size
        legend_y = y0 + pad * 2 + fonts["label"].size
        latest = tests[-1] if tests else None
        entries = [
            ("Download", COLOR_BLUE, f"{latest[1]:.0f} Mbps" if latest else None),
            ("Upload", COLOR_GREEN, f"{latest[2]:.0f} Mbps" if latest else None),
        ]
        lx = x0 + pad
        for name, color, value in entries:
            text = f"{name} {value}" if value else name
            draw.rectangle([lx, legend_y, lx + swatch, legend_y + swatch], fill=color, outline=COLOR_BLACK)
            draw.text((lx + swatch + 4, legend_y), text, font=legend_font, fill=COLOR_BLACK, anchor="la")
            lx += swatch + 6 + draw.textlength(text, font=legend_font) + 14
        if latest and latest[3] is not None:
            draw.text((lx, legend_y), f"Ping {latest[3]:.0f} ms", font=legend_font, fill=COLOR_BLACK, anchor="la")

        chart_top = legend_y + swatch + pad
        chart_bottom = y1 - pad * 2 - fonts["small"].size - 6
        chart_left = x0 + pad + 48
        chart_right = x1 - pad

        if not tests:
            draw.text(((chart_left + chart_right) / 2, (chart_top + chart_bottom) / 2), "No speed tests yet",
                      font=fonts["label"], fill=COLOR_BLUE, anchor="mm")
            return

        span = SPEEDTEST_WINDOW_HOURS * 3600

        def x_of(ts):
            return chart_left + (min(max(ts, since_ts), now_ts) - since_ts) / span * (chart_right - chart_left)

        def y_of(value):
            # Fixed scale: a burst above it is pinned to the top edge, not allowed to stretch the axis.
            return chart_bottom - (min(value, y_max) / y_max) * (chart_bottom - chart_top)

        for frac in (0.25, 0.5, 0.75):
            gy = chart_bottom - frac * (chart_bottom - chart_top)
            draw.line([(chart_left, gy), (chart_right, gy)], fill=COLOR_BLACK, width=1)
        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)

        # Tests are hours apart, so a long silence (daemon down, tests failing)
        # breaks the line instead of drawing a misleading straight run across it.
        gap_limit = max(3 * 3600, 2.5 * self._median_spacing(tests))
        for column, color, width in ((2, COLOR_GREEN, 3), (1, COLOR_BLUE, 4)):
            prev = None
            for test in tests:
                point = (x_of(test[0]), y_of(test[column]))
                if prev is not None and test[0] - prev[0] <= gap_limit:
                    draw.line([prev[1], point], fill=color, width=width)
                draw.ellipse([point[0] - 3, point[1] - 3, point[0] + 3, point[1] + 3], fill=color)
                prev = (test[0], point)

        draw.text((x0 + pad, chart_top), f"{int(y_max)}", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, chart_bottom), "0", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, (chart_top + chart_bottom) / 2), f"{int(y_max / 2)}", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        tick_y = chart_bottom + 4
        for frac, h_anchor in ((0.0, "l"), (0.25, "m"), (0.5, "m"), (0.75, "m"), (1.0, "r")):
            tick_ts = since_ts + frac * span
            tick_x = x_of(tick_ts)
            draw.line([(tick_x, chart_bottom), (tick_x, chart_bottom + 3)], fill=COLOR_BLACK, width=1)
            tick_dt = datetime.fromtimestamp(tick_ts, tz)
            label = f"{tick_dt.strftime('%a')} {tick_dt.strftime('%I %p').lstrip('0')}"
            draw.text((tick_x, tick_y), label, font=fonts["small"], fill=COLOR_BLACK, anchor=h_anchor + "a")

    @staticmethod
    def _median_spacing(tests):
        spacings = sorted(b[0] - a[0] for a, b in zip(tests, tests[1:]))
        return spacings[len(spacings) // 2] if spacings else 0

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
