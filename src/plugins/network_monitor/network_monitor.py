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
# Optional internet-usage panel: Home Assistant writes the router's cumulative
# byte counters into its own bucket (sensor.exos_router_data_received / _sent).
# It needs its own read token (INFLUXDB_HA_TOKEN); without one the panel is skipped.
DEFAULT_USAGE_BUCKET = "homeassistant"
USAGE_TOKEN_KEY = "INFLUXDB_HA_TOKEN"
USAGE_ENTITY_REGEX = "exos_router_data_(received|sent)"
USAGE_DAYS = 14
# Window of the hourly packet loss / jitter / DNS charts.
QUALITY_WINDOW_HOURS = 24
# Speed tests fetched for the monthly table (6 calendar months) and the hour/weekday charts.
MONTHLY_HISTORY_DAYS = 190
# Actual-throughput chart window, and the period over which time above the plan limit is counted.
THROUGHPUT_HOURS = 24
LIMIT_DAYS = 30
# The next-cheaper ISP package; override per instance with `limitMbps`.
DEFAULT_LIMIT_MBPS = 100
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
        self._outages = []  # [(start_ts, end_ts or None while ongoing, scope)]
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
            scope = row.get("scope") if row.get("scope") in ("isp", "local") else "unknown"
            if end_ts == 0:  # daemon writes 0 while an outage is still open
                end_ts = None
                self.outage_scope = scope
            self._outages.append((start_ts, end_ts, scope))

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

    def get_outages_scoped(self, since_ts, until_ts, now_ts):
        """[(start_ts, end_ts, scope)] overlapping the window; scope is 'isp',
        'local' or 'unknown' (e.g. history imported from before scope was tracked)."""
        # An outage still open when the daemon stopped reporting ends at the last
        # sample instead of running on to "now".
        open_end = now_ts
        if self.last_sample_ts is not None and now_ts - self.last_sample_ts > STALE_SAMPLE_SECONDS:
            open_end = self.last_sample_ts
        return [
            (start, end if end is not None else max(start, open_end), scope)
            for start, end, scope in self._outages
            if (end is None or end >= since_ts) and start <= until_ts
        ]

    def get_outages(self, since_ts, until_ts, now_ts):
        return [(start, end) for start, end, _scope in self.get_outages_scoped(since_ts, until_ts, now_ts)]

    def get_gaps(self, since_ts, until_ts):
        return [(start, end) for start, end in self._gaps if end >= since_ts and start <= until_ts]

    def get_meta_int(self, key, default):
        if key == "first_started_ts" and self._first_started_ts is not None:
            return self._first_started_ts
        return default

    def get_speedtests(self, since_ts):
        """[(ts, download_mbps, upload_mbps, idle_latency_ms or None, server or None,
        latency_under_download_ms or None, latency_under_upload_ms or None)], oldest first."""
        tests = []
        for row in self._pivoted("speedtest", _to_rfc3339(since_ts)):
            ts = _parse_influx_time(row.get("_time"))
            down, up = _parse_float(row.get("download_mbps")), _parse_float(row.get("upload_mbps"))
            if ts is None or down is None or up is None:
                continue
            tests.append((
                ts, down, up, _parse_float(row.get("idle_latency_ms")), row.get("server") or None,
                _parse_float(row.get("download_latency_ms")), _parse_float(row.get("upload_latency_ms")),
            ))
        tests.sort()
        return tests

    def _hourly(self, measurement, field, since_ts, until_ts, pipeline, fn="mean"):
        """{hour index counted from since_ts: value} for a raw-bucket field, aggregated
        server-side (a day of 1 Hz pings is ~86k points). `since_ts` should sit on a
        UTC hour boundary, which is what aggregateWindow aligns its windows to."""
        rows = self._query(
            f'from(bucket: "{_flux_escape(self.raw_bucket)}") '
            f'|> range(start: {_to_rfc3339(since_ts)}, stop: {_to_rfc3339(until_ts + 1)}) '
            f'|> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{field}") '
            f'{pipeline}|> aggregateWindow(every: 1h, fn: {fn}, createEmpty: false) '
            '|> keep(columns: ["_time", "_value"])'
        )
        hours = {}
        for row in rows:
            ts, value = _parse_influx_time(row.get("_time")), _parse_float(row.get("_value"))
            if ts is not None and value is not None:
                # _time is the window's end, so step back a second to land in its own hour.
                hours[(ts - 1 - since_ts) // 3600] = value
        return hours

    def get_quality(self, since_ts, until_ts):
        """Hourly line-quality series for the window starting at `since_ts` (a UTC hour
        boundary): packet loss %, jitter ms, and DNS lookup ms via the default and a
        public resolver. Each is {hour index: value}; hours without data are absent.
        Also returns the number of failed default-resolver lookups in the window."""
        abs_delta = (
            '|> difference() |> map(fn: (r) => ({r with _value: if r._value < 0.0 then -r._value else r._value})) '
        )
        loss = {h: v * 100 for h, v in self._hourly("ping", "miss", since_ts, until_ts, "").items()}
        jitter = self._hourly("ping", "rtt_ms", since_ts, until_ts, abs_delta)
        dns_default = self._hourly("dns", "default_ms", since_ts, until_ts, "")
        dns_public = self._hourly("dns", "public_ms", since_ts, until_ts, "")
        fail_rows = self._query(
            f'from(bucket: "{_flux_escape(self.raw_bucket)}") '
            f'|> range(start: {_to_rfc3339(since_ts)}, stop: {_to_rfc3339(until_ts + 1)}) '
            '|> filter(fn: (r) => r._measurement == "dns" and r._field == "default_ok" and r._value == 0) '
            '|> group() |> count()'
        )
        dns_failures = sum(_parse_int(row.get("_value")) or 0 for row in fail_rows)
        return {"loss": loss, "jitter": jitter, "dns_default": dns_default,
                "dns_public": dns_public, "dns_failures": dns_failures}

    def get_daily_usage(self, days, tz_name):
        """Bytes downloaded / uploaded per local calendar day for the last `days`
        days (oldest first, today last), from Home Assistant's cumulative router
        byte counters. {"dates": [date], "down": [bytes|None], "up": [bytes|None]}.
        The counters are read hourly and differenced (a reset counts as zero for
        that hour), then summed per local day server-side."""
        rows = self._query(
            'import "timezone"\n'
            f'option location = timezone.location(name: "{_flux_escape(tz_name)}")\n'
            f'from(bucket: "{_flux_escape(self.bucket)}") |> range(start: -{days + 1}d) '
            f'|> filter(fn: (r) => r._field == "value" and r.entity_id =~ /^{USAGE_ENTITY_REGEX}$/) '
            '|> aggregateWindow(every: 1h, fn: last, createEmpty: false) '
            '|> difference(nonNegative: true) '
            '|> aggregateWindow(every: 1d, fn: sum, createEmpty: true) '
            '|> keep(columns: ["_time", "_value", "entity_id"])'
        )
        tz = pytz.timezone(tz_name)
        by_day = {"down": {}, "up": {}}
        for row in rows:
            ts, value = _parse_influx_time(row.get("_time")), _parse_float(row.get("_value"))
            if ts is None or value is None:
                continue
            # aggregateWindow stamps a window with its end, so step back into the day it covers.
            day = datetime.fromtimestamp(ts - 3600, tz).date()
            key = "down" if row.get("entity_id", "").endswith("received") else "up"
            by_day[key][day] = by_day[key].get(day, 0.0) + value
        today = datetime.now(tz).date()
        dates = [today - timedelta(days=d) for d in range(days - 1, -1, -1)]
        return {"dates": dates,
                "down": [by_day["down"].get(d) for d in dates],
                "up": [by_day["up"].get(d) for d in dates]}


    def _rate_flux(self, range_args, direction=None):
        """Flux building the router's throughput in Mbit/s per minute (the byte counters
        are logged about every 30 s; a counter reset counts as zero for that minute)."""
        entity = USAGE_ENTITY_REGEX if direction is None else f"exos_router_data_{direction}"
        return (
            f'from(bucket: "{_flux_escape(self.bucket)}") |> range({range_args}) '
            f'|> filter(fn: (r) => r._field == "value" and r.entity_id =~ /^{entity}$/) '
            '|> aggregateWindow(every: 1m, fn: last, createEmpty: false) '
            '|> derivative(unit: 1s, nonNegative: true) '
            '|> map(fn: (r) => ({r with _value: r._value * 8.0 / 1000000.0})) '
        )

    def get_throughput(self, since_ts, until_ts):
        """Per-minute actual throughput over the window: {"down": [(ts, mbps)], "up": [...]}."""
        rows = self._query(
            self._rate_flux(f"start: {_to_rfc3339(since_ts)}, stop: {_to_rfc3339(until_ts + 1)}")
            + '|> keep(columns: ["_time", "_value", "entity_id"])'
        )
        series = {"down": [], "up": []}
        for row in rows:
            ts, value = _parse_influx_time(row.get("_time")), _parse_float(row.get("_value"))
            if ts is not None and value is not None:
                series["down" if row.get("entity_id", "").endswith("received") else "up"].append((ts, value))
        for points in series.values():
            points.sort()
        return series

    def get_limit_stats(self, days, tz_name, limit_mbps):
        """How often the link ran above `limit_mbps` (and at 50% / 80% of it) in the last
        `days` days, from per-minute throughput. Returns
        {"dates": [date], "down_min": [minutes|None], "up_min": [...],
         "tiers": {mbps: down minutes}, "peak_down": Mbit/s, "peak_up": Mbit/s, "data_days": float|None}."""
        zone = (f'import "timezone"\noption location = timezone.location(name: "{_flux_escape(tz_name)}")\n')

        def flagged(threshold, direction=None):
            return (self._rate_flux(f"start: -{days}d", direction)
                    + f'|> map(fn: (r) => ({{r with _value: if r._value > {threshold:.3f} then 1.0 else 0.0}})) ')

        tz = pytz.timezone(tz_name)
        per_day = {"down": {}, "up": {}}
        daily = self._query(zone + flagged(limit_mbps)
                            + '|> aggregateWindow(every: 1d, fn: sum, createEmpty: true) '
                            '|> keep(columns: ["_time", "_value", "entity_id"])')
        for row in daily:
            ts, value = _parse_influx_time(row.get("_time")), _parse_float(row.get("_value"))
            if ts is None or value is None:
                continue
            day = datetime.fromtimestamp(ts - 3600, tz).date()  # window stamped with its end
            key = "down" if row.get("entity_id", "").endswith("received") else "up"
            per_day[key][day] = per_day[key].get(day, 0.0) + value

        tiers = {}
        for fraction in (0.5, 0.8):
            rows = self._query(flagged(limit_mbps * fraction, "received")
                               + '|> group() |> sum() |> keep(columns: ["_value"])')
            tiers[limit_mbps * fraction] = (_parse_float(rows[0].get("_value")) if rows else None) or 0.0

        peaks = {"down": 0.0, "up": 0.0}
        for row in self._query(self._rate_flux(f"start: -{days}d")
                               + '|> group(columns: ["entity_id"]) |> max() |> keep(columns: ["_value", "entity_id"])'):
            value = _parse_float(row.get("_value"))
            if value is not None:
                peaks["down" if row.get("entity_id", "").endswith("received") else "up"] = value

        first = self._query(
            f'from(bucket: "{_flux_escape(self.bucket)}") |> range(start: -{days}d) '
            f'|> filter(fn: (r) => r._field == "value" and r.entity_id =~ /^exos_router_data_received$/) '
            '|> group() |> first() |> keep(columns: ["_time"])'
        )
        first_ts = _parse_influx_time(first[0].get("_time")) if first else None
        today = datetime.now(tz).date()
        dates = [today - timedelta(days=d) for d in range(days - 1, -1, -1)]
        return {"dates": dates,
                "down_min": [per_day["down"].get(d) for d in dates],
                "up_min": [per_day["up"].get(d) for d in dates],
                "tiers": tiers, "peak_down": peaks["down"], "peak_up": peaks["up"],
                "data_days": None if first_ts is None else (time.time() - first_ts) / 86400}


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
                "big": get_font("Jost", round(height * 0.034), "bold"),
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
            speed_box = (left_width + margin // 2, right_top, width - margin, right_top + round(right_height * 0.50))
            limit_box = (left_width + margin // 2, right_top + round(right_height * 0.50) + margin, width - margin, right_bottom)

            min_outage_seconds = self._parse_min_outage_seconds(settings)
            speed_max = self._parse_speed_max(settings)
            limit_mbps = self._parse_limit_mbps(settings)

            # One query feeds the monthly speed-test table.
            all_tests = store.get_speedtests(now_ts - MONTHLY_HISTORY_DAYS * 86400)

            usage = self._load_usage(settings, device_config, influx_url, influx_org, raw_bucket, verify_ssl, timezone_name)
            self._draw_left_column(draw, left_box, now_ts, tz, fonts, min_outage_seconds, time_format, speed_max, all_tests, usage)
            traffic = self._load_traffic(settings, device_config, influx_url, influx_org, raw_bucket, verify_ssl,
                                         timezone_name, now_ts, limit_mbps)
            self._draw_throughput_panel(draw, speed_box, traffic, now_ts, tz, fonts, time_format, limit_mbps)
            self._draw_limit_panel(draw, limit_box, traffic, fonts, limit_mbps)
        except InfluxQueryError as e:
            logger.error(f"Failed to query InfluxDB: {e}")
            raise RuntimeError("Failed to retrieve network monitor data from InfluxDB, please check logs.")

        return image

    @staticmethod
    def _load_usage(settings, device_config, url, org, raw_bucket, verify_ssl, tz_name):
        """Daily usage for the optional panel, or None when it isn't configured or
        can't be read (a missing panel must never stop the rest of the display)."""
        token = device_config.load_env_key(USAGE_TOKEN_KEY)
        if not token:
            return None
        bucket = (settings.get("usageBucket") or "").strip() or DEFAULT_USAGE_BUCKET
        try:
            return _InfluxStore(url, org, token, raw_bucket, bucket, verify_ssl).get_daily_usage(USAGE_DAYS, tz_name)
        except InfluxQueryError as e:
            logger.warning(f"Internet usage panel skipped: {e}")
            return None

    @staticmethod
    def _load_traffic(settings, device_config, url, org, raw_bucket, verify_ssl, tz_name, now_ts, limit_mbps):
        """Actual throughput (last 24 h) and time-above-limit stats (last 30 days) from the
        router byte counters, or None when unavailable (never stops the rest of the display)."""
        token = device_config.load_env_key(USAGE_TOKEN_KEY)
        if not token:
            return None
        bucket = (settings.get("usageBucket") or "").strip() or DEFAULT_USAGE_BUCKET
        store = _InfluxStore(url, org, token, raw_bucket, bucket, verify_ssl)
        try:
            return {"series": store.get_throughput(now_ts - THROUGHPUT_HOURS * 3600, now_ts),
                    "limit": store.get_limit_stats(LIMIT_DAYS, tz_name, limit_mbps)}
        except InfluxQueryError as e:
            logger.warning(f"Throughput panels skipped: {e}")
            return None

    @staticmethod
    def _parse_limit_mbps(settings):
        try:
            value = float(settings.get("limitMbps"))
        except (TypeError, ValueError):
            value = DEFAULT_LIMIT_MBPS
        return value if value > 0 else DEFAULT_LIMIT_MBPS

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

    def _draw_left_column(self, draw, box, now_ts, tz, fonts, min_outage_seconds, time_format, speed_max_mbps, tests, usage=None):
        """Monthly speed table with the latest reading (top), hourly line quality,
        the daily internet usage (when configured) and the 12-hour ping sparkline (bottom)."""
        x0, y0, x1, y1 = box
        gap = round((y1 - y0) * 0.03)
        # The quality panel is the busiest (title, notes and three charts), the ping line the simplest.
        weights = (1.0, 1.15, 0.85) if usage is None else (0.95, 1.05, 0.9, 0.7)
        unit = (y1 - y0 - (len(weights) - 1) * gap) / sum(weights)
        boxes = []
        top = y0
        for weight in weights:
            boxes.append((x0, top, x1, top + unit * weight))
            top += unit * weight + gap
        stats_box, quality_box = boxes[0], boxes[1]
        ping_box = boxes[-1]
        self._draw_stats_table(draw, stats_box, tests, tz, now_ts, fonts, speed_max_mbps, time_format)
        self._draw_quality_panel(draw, quality_box, now_ts, tz, fonts, time_format)
        if usage is not None:
            self._draw_usage_panel(draw, boxes[2], usage, fonts)

        since_ts = now_ts - 12 * 3600
        samples = self._store.get_samples(since_ts, now_ts)
        outages = self._filter_outages(self._store.get_outages(since_ts, now_ts, now_ts), min_outage_seconds)
        self._draw_sparkline_panel(draw, ping_box, "Last 12 Hours", since_ts, now_ts, samples, outages, fonts, tz, time_format)

    def _draw_quality_panel(self, draw, box, now_ts, tz, fonts, time_format):
        """Hourly packet loss, jitter and DNS lookup time over the last
        QUALITY_WINDOW_HOURS, as three side-by-side mini charts."""
        x0, y0, x1, y1 = box
        pad = 8
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), f"Line Quality - Last {QUALITY_WINDOW_HOURS} Hours",
                  font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        hours = QUALITY_WINDOW_HOURS
        # Hour-aligned (UTC, like the server-side windows) and ending with the current partial hour.
        since_ts = (now_ts // 3600) * 3600 - (hours - 1) * 3600
        data = self._store.get_quality(since_ts, now_ts)

        def series(key):
            return [data[key].get(h) for h in range(hours)]

        loss, jitter = series("loss"), series("jitter")
        dns_default, dns_public = series("dns_default"), series("dns_public")

        top = y0 + pad * 2 + fonts["label"].size
        col_gap = pad * 2
        col_w = (x1 - x0 - 2 * pad - 2 * col_gap) / 3
        cols = [(x0 + pad + i * (col_w + col_gap), x0 + pad + i * (col_w + col_gap) + col_w) for i in range(3)]
        bottom = y1 - pad

        def stats(values, unit, digits=1):
            present = [v for v in values if v is not None]
            if not present:
                return "no data yet"
            return f"avg {sum(present) / len(present):.{digits}f}{unit}, max {max(present):.{digits}f}{unit}"

        def tick_label(frac):
            dt = datetime.fromtimestamp(since_ts + frac * (hours - 1) * 3600, tz)
            return dt.strftime("%H:00") if time_format == "24h" else dt.strftime("%I %p").lstrip("0")

        dns_present = [v for v in dns_default + dns_public if v is not None]
        dns_note = (f"{data['dns_failures']} failed" if data["dns_failures"] else
                    ("avg " + f"{sum(dns_present) / len(dns_present):.0f} ms" if dns_present else "no data yet"))
        charts = [
            ("Packet Loss %", stats(loss, "%"), [(loss, COLOR_RED)], "bars", 1.0, None),
            ("Jitter ms", stats(jitter, " ms"), [(jitter, COLOR_BLUE)], "bars", 5.0, None),
            ("DNS Lookup ms", dns_note, [(dns_default, COLOR_BLUE), (dns_public, COLOR_GREEN)], "lines", 20.0,
             [("Default", COLOR_BLUE), ("Public", COLOR_GREEN)]),
        ]
        for (cx0, cx1), (title, note, lines, kind, min_max, legend) in zip(cols, charts):
            draw.text((cx0, top), title, font=fonts["label"], fill=COLOR_BLACK, anchor="la")
            note_y = top + round(fonts["label"].size * 1.3)
            small = fonts["small"]
            lx = cx0
            if legend:
                for name, color in legend:
                    draw.rectangle([lx, note_y + 1, lx + small.size - 2, note_y + small.size - 1], fill=color, outline=COLOR_BLACK)
                    draw.text((lx + small.size + 2, note_y), name, font=small, fill=COLOR_BLACK, anchor="la")
                    lx += small.size + 14 + draw.textlength(name, font=small)
            draw.text((lx, note_y), note, font=small, fill=COLOR_BLACK, anchor="la")

            chart_top = note_y + round(small.size * 1.4) + small.size // 2
            chart_bottom = bottom - small.size - 6
            chart_left = cx0 + 40
            self._draw_mini_chart(draw, (chart_left, chart_top, cx1, chart_bottom), lines, kind, min_max, fonts, hours)
            tick_y = chart_bottom + 4
            for frac, h_anchor, tx in ((0.0, "l", chart_left), (0.5, "m", (chart_left + cx1) / 2), (1.0, "r", cx1)):
                draw.line([(tx, chart_bottom), (tx, chart_bottom + 3)], fill=COLOR_BLACK, width=1)
                draw.text((tx, tick_y), tick_label(frac), font=small, fill=COLOR_BLACK, anchor=h_anchor + "a")

    def _draw_usage_panel(self, draw, box, usage, fonts):
        """Daily downloaded / uploaded gigabytes as paired bars, oldest day left."""
        x0, y0, x1, y1 = box
        pad = 8
        label, small = fonts["label"], fonts["small"]
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        days = len(usage["dates"])
        draw.text((x0 + pad, y0 + pad), f"Internet Usage (GB) - Last {days} Days", font=label, fill=COLOR_BLACK, anchor="la")

        gb = lambda values: [None if v is None else v / 1e9 for v in values]
        down, up = gb(usage["down"]), gb(usage["up"])
        note_y = y0 + pad
        lx = x1 - pad
        # Legend and today's totals, right-aligned on the title row.
        today = f"Today {down[-1] or 0:.1f} down / {up[-1] or 0:.1f} up GB"
        draw.text((lx, note_y + label.size * 0.1), today, font=small, fill=COLOR_BLACK, anchor="ra")
        lx -= draw.textlength(today, font=small) + 14
        for name, color in (("Up", COLOR_GREEN), ("Down", COLOR_BLUE)):
            lx -= draw.textlength(name, font=small)
            draw.text((lx, note_y + label.size * 0.1), name, font=small, fill=COLOR_BLACK, anchor="la")
            lx -= small.size + 2
            draw.rectangle([lx, note_y + label.size * 0.1 + 1, lx + small.size - 2, note_y + label.size * 0.1 + small.size - 1],
                           fill=color, outline=COLOR_BLACK)
            lx -= 12

        chart_top = y0 + pad * 2 + label.size + round(small.size * 0.6)
        chart_bottom = y1 - pad - small.size - 6
        chart_left, chart_right = x0 + pad + 44, x1 - pad
        present = [v for v in down + up if v is not None]
        if not present:
            draw.text(((chart_left + chart_right) / 2, (chart_top + chart_bottom) / 2), "Collecting usage data...",
                      font=label, fill=COLOR_BLUE, anchor="mm")
            return
        peak = max(present)
        # Round the axis up to a 1/2/5 step so the gridlines read as whole numbers.
        magnitude = 10 ** len(str(int(max(peak, 1))))
        top_value = max(1.0, next((m * magnitude / 10 for m in (1, 2, 2.5, 5, 10) if m * magnitude / 10 >= peak), peak))

        def y_of(value):
            return chart_bottom - (value / top_value) * (chart_bottom - chart_top)

        for frac in (0.25, 0.5, 0.75):
            gy = chart_bottom - frac * (chart_bottom - chart_top)
            draw.line([(chart_left, gy), (chart_right, gy)], fill=COLOR_BLACK, width=1)
        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)
        draw.text((chart_left - 6, chart_top), f"{top_value:g}", font=small, fill=COLOR_BLACK, anchor="rm")
        draw.text((chart_left - 6, chart_bottom), "0", font=small, fill=COLOR_BLACK, anchor="rm")

        slot_w = (chart_right - chart_left) / days
        for i in range(days):
            for j, (values, color) in enumerate(((down, COLOR_BLUE), (up, COLOR_GREEN))):
                value = values[i]
                if not value:
                    continue
                bx0 = chart_left + i * slot_w + slot_w * (0.1 + 0.4 * j)
                bx1 = bx0 + slot_w * 0.4
                draw.rectangle([bx0, y_of(min(value, top_value)), bx1, chart_bottom], fill=color, outline=COLOR_BLACK)
        for idx, anchor in ((0, "l"), (days // 2, "m"), (days - 1, "r")):
            tx = chart_left + (idx + 0.5) * slot_w
            tx = {"l": chart_left, "m": tx, "r": chart_right}[anchor]
            draw.text((tx, chart_bottom + 4), usage["dates"][idx].strftime("%b %d").replace(" 0", " "),
                      font=small, fill=COLOR_BLACK, anchor=anchor + "a")

    @staticmethod
    def _draw_mini_chart(draw, box, lines, kind, min_max, fonts, slots):
        """One hourly series set in `box` (bars or connected dots), with a y-axis
        that spans 0 to at least `min_max`. `lines` is [(values per slot, color)]."""
        x0, y0, x1, y1 = box
        small = fonts["small"]
        present = [v for values, _color in lines for v in values if v is not None]
        top_value = max([min_max] + present)

        def y_of(value):
            return y1 - (value / top_value) * (y1 - y0)

        for frac in (0.25, 0.5, 0.75):
            gy = y1 - frac * (y1 - y0)
            draw.line([(x0, gy), (x1, gy)], fill=COLOR_BLACK, width=1)
        draw.line([(x0, y1), (x1, y1)], fill=COLOR_BLACK, width=1)
        draw.text((x0 - 6, y0), f"{top_value:.0f}" if top_value >= 10 else f"{top_value:.1f}",
                  font=small, fill=COLOR_BLACK, anchor="rm")
        draw.text((x0 - 6, y1), "0", font=small, fill=COLOR_BLACK, anchor="rm")

        if not present:
            draw.text(((x0 + x1) / 2, (y0 + y1) / 2), "No data yet", font=fonts["label"], fill=COLOR_BLUE, anchor="mm")
            return

        slot_w = (x1 - x0) / slots
        for values, color in lines:
            prev = None
            for i, value in enumerate(values):
                if value is None:
                    prev = None
                    continue
                cx = x0 + (i + 0.5) * slot_w
                if kind == "bars":
                    bar_h = max(2, y1 - y_of(value)) if value > 0 else 0
                    if bar_h:
                        draw.rectangle([cx - slot_w * 0.35, y1 - bar_h, cx + slot_w * 0.35, y1], fill=color, outline=COLOR_BLACK)
                else:
                    point = (cx, y_of(value))
                    if prev is not None:
                        draw.line([prev, point], fill=color, width=3)
                    draw.ellipse([point[0] - 3, point[1] - 3, point[0] + 3, point[1] + 3], fill=color)
                    prev = point

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

    @staticmethod
    def _bucket_series(points, since_ts, buckets, bucket_seconds):
        """Per-minute (ts, mbps) points -> (mean list, peak list) over `buckets` equal slots
        starting at `since_ts`; a slot with no points is None."""
        slots = [[] for _ in range(buckets)]
        for ts, value in points:
            idx = int((ts - since_ts) // bucket_seconds)
            if 0 <= idx < buckets:
                slots[idx].append(value)
        means = [sum(v) / len(v) if v else None for v in slots]
        peaks = [max(v) if v else None for v in slots]
        return means, peaks

    @staticmethod
    def _nice_top(value):
        """Round an axis top up to a 1/2/2.5/5/10 step so gridlines read as round numbers."""
        value = max(value, 1.0)
        magnitude = 10 ** (len(str(int(value))) - 1)
        return next(m * magnitude for m in (1, 2, 2.5, 5, 10) if m * magnitude >= value)

    @staticmethod
    def _dashed_hline(draw, x0, x1, y, color, dash=6, gap=5):
        x = x0
        while x < x1:
            draw.line([(x, y), (min(x + dash, x1), y)], fill=color, width=2)
            x += dash + gap

    @staticmethod
    def _fmt_minutes(minutes):
        minutes = int(round(minutes or 0))
        if minutes >= 120:
            return f"{minutes // 60} h {minutes % 60:02d} min"
        return f"{minutes} min"

    def _draw_throughput_panel(self, draw, box, traffic, now_ts, tz, fonts, time_format, limit_mbps):
        """Actual download and upload speed over the last THROUGHPUT_HOURS: bars are 15-minute
        averages, the black ticks the fastest single minute inside each slot."""
        x0, y0, x1, y1 = box
        pad = 10
        label, small = fonts["label"], fonts["small"]
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), f"Actual Speed - Last {THROUGHPUT_HOURS} Hours (Mbit/s)",
                  font=label, fill=COLOR_BLACK, anchor="la")
        if traffic is None or not (traffic["series"]["down"] or traffic["series"]["up"]):
            draw.text(((x0 + x1) / 2, (y0 + y1) / 2), "Collecting speed data...", font=label, fill=COLOR_BLUE, anchor="mm")
            return

        bucket_s = 900
        buckets = THROUGHPUT_HOURS * 3600 // bucket_s
        since_ts = now_ts - THROUGHPUT_HOURS * 3600
        chart_left, chart_right = x0 + pad + 40, x1 - pad
        body_top = y0 + pad * 2 + label.size
        tick_room = small.size + 12
        sub_h = (y1 - pad - tick_room - body_top) / 2
        for n, (key, name, color) in enumerate((("down", "Download", COLOR_BLUE), ("up", "Upload", COLOR_GREEN))):
            top = body_top + n * sub_h
            means, peaks = self._bucket_series(traffic["series"][key], since_ts, buckets, bucket_s)
            present = [v for v in means if v is not None]
            peak = max([p for p in peaks if p is not None], default=0.0)
            note = f"avg {sum(present) / len(present):.1f}, peak {peak:.0f}" if present else "no data"
            draw.text((chart_left, top), name, font=small, fill=COLOR_BLACK, anchor="la")
            draw.text((chart_right, top), note, font=small, fill=COLOR_BLACK, anchor="ra")
            c_top, c_bottom = top + small.size + 8, top + sub_h - 10
            # Scaled to the data; the limit line appears once traffic gets near it.
            top_value = self._nice_top(max(peak, 10.0))

            def y_of(value, c_top=c_top, c_bottom=c_bottom, top_value=top_value):
                return c_bottom - (min(value, top_value) / top_value) * (c_bottom - c_top)

            for frac in (0.5, 1.0):
                gy = c_bottom - frac * (c_bottom - c_top)
                draw.line([(chart_left, gy), (chart_right, gy)], fill=COLOR_BLACK, width=1)
            draw.text((chart_left - 6, c_top), f"{top_value:g}", font=small, fill=COLOR_BLACK, anchor="rm")
            draw.text((chart_left - 6, c_bottom), "0", font=small, fill=COLOR_BLACK, anchor="rm")
            slot_w = (chart_right - chart_left) / buckets
            for i, (mean, slot_peak) in enumerate(zip(means, peaks)):
                if mean is None:
                    continue
                bx0 = chart_left + i * slot_w
                bx1 = bx0 + max(1.0, slot_w - 1)
                if y_of(mean) < c_bottom - 0.5:
                    draw.rectangle([bx0, y_of(mean), bx1, c_bottom], fill=color)
                draw.line([(bx0, y_of(slot_peak)), (bx1, y_of(slot_peak))], fill=COLOR_BLACK, width=2)
            if limit_mbps <= top_value:
                ly = y_of(limit_mbps)
                self._dashed_hline(draw, chart_left, chart_right, ly, COLOR_RED)
                draw.text((chart_left - 6, ly), f"{limit_mbps:g}", font=small, fill=COLOR_RED, anchor="rm")
            draw.line([(chart_left, c_bottom), (chart_right, c_bottom)], fill=COLOR_BLACK, width=1)
            draw.line([(chart_left, c_top), (chart_left, c_bottom)], fill=COLOR_BLACK, width=1)

        axis_y = body_top + 2 * sub_h - 10
        # Ticks on round local hours (every 6 h) so the labels never crowd each other.
        tick_ts = (since_ts // 3600 + 1) * 3600
        while tick_ts <= now_ts:
            tick_dt = datetime.fromtimestamp(tick_ts, tz)
            if tick_dt.hour % 6 == 0:
                tx = chart_left + (tick_ts - since_ts) / (THROUGHPUT_HOURS * 3600) * (chart_right - chart_left)
                draw.line([(tx, axis_y), (tx, axis_y + 3)], fill=COLOR_BLACK, width=1)
                text = f"{tick_dt.hour:02d}:00" if time_format == "24h" else tick_dt.strftime("%I %p").lstrip("0")
                draw.text((tx, axis_y + 5), text, font=small, fill=COLOR_BLACK, anchor="ma")
            tick_ts += 3600

    def _draw_limit_panel(self, draw, box, traffic, fonts, limit_mbps):
        """How much of the last LIMIT_DAYS the link ran above `limit_mbps` (the next ISP
        package down), as totals and minutes per day."""
        x0, y0, x1, y1 = box
        pad = 10
        label, small = fonts["label"], fonts["small"]
        big = fonts.get("big", label)
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), f"Time Above {limit_mbps:g} Mbit/s - Last {LIMIT_DAYS} Days",
                  font=label, fill=COLOR_BLACK, anchor="la")
        stats = traffic["limit"] if traffic else None
        if stats is None:
            draw.text(((x0 + x1) / 2, (y0 + y1) / 2), "Collecting speed data...", font=label, fill=COLOR_BLUE, anchor="mm")
            return

        down, up = stats["down_min"], stats["up_min"]
        down_total, up_total = sum(v or 0 for v in down), sum(v or 0 for v in up)
        data_days = stats["data_days"] or 0
        span_min = max(1.0, min(data_days, LIMIT_DAYS) * 1440)
        days_hit = sum(1 for v in down if v and v >= 1)
        days_seen = max(1, min(LIMIT_DAYS, int(data_days) + 1))

        y = y0 + pad * 2 + label.size + 4
        headline = self._fmt_minutes(down_total)
        draw.text((x0 + pad, y), headline, font=big, fill=COLOR_RED if down_total else COLOR_GREEN, anchor="la")
        headline_w = draw.textlength(headline, font=big)
        draw.text((x0 + pad + headline_w + 12, y + big.size * 0.55),
                  f"download = {100.0 * down_total / span_min:.2f}% of the time", font=small, fill=COLOR_BLACK, anchor="lm")
        y += big.size + 10
        lines = [
            f"Upload above {limit_mbps:g}: {self._fmt_minutes(up_total)}   |   Days hit: {days_hit} of {days_seen}",
            "   |   ".join(f"Above {mbps:g}: {self._fmt_minutes(mins)}" for mbps, mins in sorted(stats["tiers"].items())),
            f"Fastest minute: {stats['peak_down']:.0f} down / {stats['peak_up']:.0f} up Mbit/s   |   {data_days:.1f} days of data",
        ]
        for line in lines:
            while draw.textlength(line, font=small) > x1 - x0 - 2 * pad and "   |   " in line:
                line = line.rsplit("   |   ", 1)[0]
            draw.text((x0 + pad, y), line, font=small, fill=COLOR_BLACK, anchor="la")
            y += small.size + 5

        # Minutes per day above the limit (download blue, upload green).
        c_top, c_bottom = y + small.size + 8, y1 - pad - small.size - 10
        chart_left, chart_right = x0 + pad + 40, x1 - pad
        draw.text((chart_left, y), "Minutes per day above the limit", font=small, fill=COLOR_BLACK, anchor="la")
        top_value = self._nice_top(max([v for v in down + up if v is not None] + [10.0]))
        days = len(stats["dates"])
        slot_w = (chart_right - chart_left) / days
        for frac in (0.5, 1.0):
            gy = c_bottom - frac * (c_bottom - c_top)
            draw.line([(chart_left, gy), (chart_right, gy)], fill=COLOR_BLACK, width=1)
        draw.text((chart_left - 6, c_top), f"{top_value:g}", font=small, fill=COLOR_BLACK, anchor="rm")
        draw.text((chart_left - 6, c_bottom), "0", font=small, fill=COLOR_BLACK, anchor="rm")
        for i in range(days):
            for j, (values, color) in enumerate(((down, COLOR_BLUE), (up, COLOR_GREEN))):
                value = values[i]
                if not value:
                    continue
                bx0 = chart_left + i * slot_w + slot_w * (0.08 + 0.42 * j)
                top_y = c_bottom - (min(value, top_value) / top_value) * (c_bottom - c_top)
                draw.rectangle([bx0, min(top_y, c_bottom - 2), bx0 + slot_w * 0.42, c_bottom], fill=color)
        draw.line([(chart_left, c_bottom), (chart_right, c_bottom)], fill=COLOR_BLACK, width=1)
        draw.line([(chart_left, c_top), (chart_left, c_bottom)], fill=COLOR_BLACK, width=1)
        for idx, anchor in ((0, "l"), (days // 2, "m"), (days - 1, "r")):
            tx = {"l": chart_left, "m": chart_left + (idx + 0.5) * slot_w, "r": chart_right}[anchor]
            draw.text((tx, c_bottom + 5), stats["dates"][idx].strftime("%b %d").replace(" 0", " "),
                      font=small, fill=COLOR_BLACK, anchor=anchor + "a")

    @staticmethod
    def _monthly_stats(tests, tz, now_ts, months, plan_mbps):
        """Per-month rows, newest first: (label, avg down, avg up, min down,
        % of tests under 80% of plan, avg idle latency, test count); a month
        with no tests has None for every statistic."""
        now_dt = datetime.fromtimestamp(now_ts, tz)
        buckets = {}
        for ts, down, up, latency, *_rest in tests:
            dt = datetime.fromtimestamp(ts, tz)
            buckets.setdefault((dt.year, dt.month), []).append((down, up, latency))
        rows = []
        year, month = now_dt.year, now_dt.month
        for _ in range(months):
            data = buckets.get((year, month), [])
            label = datetime(year, month, 1).strftime("%b %y")
            if data:
                downs, ups = [d[0] for d in data], [d[1] for d in data]
                lats = [d[2] for d in data if d[2] is not None]
                slow = 100.0 * sum(1 for d in downs if d < 0.8 * plan_mbps) / len(downs)
                rows.append((label, sum(downs) / len(downs), sum(ups) / len(ups), min(downs), slow,
                             sum(lats) / len(lats) if lats else None, len(data)))
            else:
                rows.append((label, None, None, None, None, None, 0))
            month -= 1
            if month == 0:
                year, month = year - 1, 12
        return rows

    def _latest_reading(self, test, tz, time_format):
        """Text lines (longest first) describing the newest speed test."""
        if test is None:
            return ["Latest: no speed tests yet"]
        dt = datetime.fromtimestamp(test[0], tz)
        parts = [f"Latest {dt.strftime('%a')} {self._format_clock_time(dt, time_format)}",
                 f"Down {test[1]:.0f} Mbps", f"Up {test[2]:.0f} Mbps"]
        if test[3] is not None:
            parts.append(f"Ping {test[3]:.0f} ms")
        full = list(parts)
        loaded = [v for v in test[5:7] if v is not None]
        if loaded:
            full.append("Under load " + "/".join(f"{v:.0f}" for v in loaded) + " ms")
        return ["   |   ".join(full), "   |   ".join(parts)]

    def _draw_stats_table(self, draw, box, tests, tz, now_ts, fonts, plan_mbps, time_format="12h"):
        """Month-by-month speed summary, to show how the link changes as the ISP
        adds customers to the fiber line, headed by the newest reading."""
        x0, y0, x1, y1 = box
        pad = 8
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), f"Monthly Speed Summary (Plan {int(plan_mbps)} Mbps)",
                  font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        small = fonts["small"]
        latest_y = y0 + pad * 2 + fonts["label"].size
        for text in self._latest_reading(tests[-1] if tests else None, tz, time_format):
            if draw.textlength(text, font=small) <= x1 - x0 - 2 * pad:
                break
        draw.text((x0 + pad, latest_y), text, font=small, fill=COLOR_BLACK, anchor="la")
        top = latest_y + small.size + pad
        rows = self._monthly_stats(tests, tz, now_ts, 6, plan_mbps)
        row_h = (y1 - pad - top) / (len(rows) + 1)

        headers = ["Month", "Avg Down", "Avg Up", "Min Down", "<80% Plan", "Ping", "Tests"]
        left, right = x0 + pad, x1 - pad
        col_w = (right - left) / len(headers)

        def cell_x(c):
            # first column left-aligned, the rest right-aligned to their column edge
            return left if c == 0 else left + (c + 1) * col_w - 4

        for c, text in enumerate(headers):
            draw.text((cell_x(c), top + row_h / 2), text, font=small, fill=COLOR_BLACK, anchor="lm" if c == 0 else "rm")
        draw.line([(left, top + row_h), (right, top + row_h)], fill=COLOR_BLACK, width=2)

        for r, (label, down, up, min_down, slow, latency, count) in enumerate(rows):
            cy = top + (r + 1.5) * row_h
            if r:
                draw.line([(left, cy - row_h / 2), (right, cy - row_h / 2)], fill=COLOR_BLACK, width=1)
            if down is None:
                draw.text((cell_x(0), cy), label, font=small, fill=COLOR_BLACK, anchor="lm")
                draw.text((cell_x(1), cy), "no data", font=small, fill=COLOR_BLACK, anchor="rm")
                continue
            # Avg Down gets a background keyed to its share of the plan so a drop reads at a glance.
            pct = down / plan_mbps
            color = COLOR_GREEN if pct >= 0.8 else COLOR_YELLOW if pct >= 0.5 else COLOR_RED
            cells = [label, f"{down:.0f} ({pct * 100:.0f}%)", f"{up:.0f}", f"{min_down:.0f}", f"{slow:.0f}%",
                     f"{latency:.0f} ms" if latency is not None else "-", str(count)]
            for c, text in enumerate(cells):
                text_color = COLOR_BLACK
                if c == 1:
                    tw = draw.textlength(text, font=small)
                    draw.rectangle([cell_x(c) - tw - 3, cy - row_h / 2 + 1, cell_x(c) + 2, cy + row_h / 2 - 1], fill=color)
                    if color == COLOR_RED:
                        text_color = COLOR_WHITE
                draw.text((cell_x(c), cy), text, font=small, fill=text_color, anchor="lm" if c == 0 else "rm")
