import csv
import io
import logging
import math
import re
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
import requests
from PIL import Image, ImageDraw

from plugins.base_plugin.base_plugin import BasePlugin
from utils.app_utils import get_font

logger = logging.getLogger(__name__)

DEFAULT_INFLUX_URL = "http://PiDisplay.local:8086"
DEFAULT_INFLUX_ORG = "none"
DEFAULT_BUCKET = "forecast"
DEFAULT_LOCATION = "home"
DEFAULT_TITLE = "Forecast Accuracy"
DEFAULT_WINDOW_DAYS = 30
DEFAULT_CONE_DAYS_AHEAD = 5
REQUEST_TIMEOUT = 30

# Accuracy table columns: lead time in days (a forecast made ~N days before the day it is for).
TABLE_LEADS = [1, 2, 3, 5, 7, 10, 14]
MIN_SAMPLES = 2          # fewer comparisons than this and a cell shows "-"
CONE_MIN_SPAN_DAYS = 5   # a completed day needs this much forecast history to make a useful cone
CONE_MIN_MODELS = 2
BAND_BUCKET_HOURS = 6

COLOR_BLACK = "#000000"
COLOR_WHITE = "#ffffff"
COLOR_RED = "#a6242b"
COLOR_YELLOW = "#dfaf2c"
COLOR_BLUE = "#345f94"
COLOR_GREEN = "#428c46"
COLOR_BAND = "#d9d9d9"
COLOR_GRID = "#bdbdbd"

# model tag -> (short label, color, dash pattern as (on, off) pixels or None for solid, line width factor)
# Several models share a color, so the dash pattern is what tells them apart; the table doubles as the legend.
MODEL_STYLES = {
    "gfs_global": ("GFS", COLOR_BLUE, None, 1),
    "ecmwf_ifs025": ("ECMWF", COLOR_RED, None, 1),
    "ecmwf_aifs025_single": ("AIFS", COLOR_RED, (14, 8), 1),
    "icon_global": ("ICON", COLOR_GREEN, None, 1),
    "jma_gsm": ("JMA", COLOR_YELLOW, None, 1),
    "ukmo_global_deterministic_10km": ("UKMO", COLOR_BLACK, (14, 8), 1),
    "meteofrance_arpege_world": ("ARPEGE", COLOR_GREEN, (14, 8), 1),
    "ncep_rrfs_conus": ("RRFS", COLOR_BLUE, (14, 8), 1),
    "gem_hrdps_continental": ("HRDPS", COLOR_BLACK, (3, 7), 1),
    "ncep_hrrr_conus": ("HRRR", COLOR_BLUE, (3, 7), 1),
    "met_norway": ("MET.no", COLOR_YELLOW, (14, 8), 1),
    "eccc": ("ECCC", COLOR_BLACK, None, 2),
    "owm": ("OWM", COLOR_GREEN, (3, 7), 1),
}
FALLBACK_STYLE = (None, COLOR_BLACK, (6, 6), 1)

# metric -> (title, field, unit label metric, unit label imperial)
METRICS = {
    "tmax": ("High temperature", "tmax", "°C", "°F"),
    "tmin": ("Low temperature", "tmin", "°C", "°F"),
    "precip": ("Precipitation", "precip", "mm", "in"),
    "wind": ("Peak wind", "wind", "km/h", "mph"),
}


class InfluxQueryError(RuntimeError):
    pass


def _flux_escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _parse_annotated_csv(text):
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


def _parse_influx_time(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _style(model):
    label, color, dash, width = MODEL_STYLES.get(model, FALLBACK_STYLE)
    return (label or model, color, dash, width)


def model_order(models):
    known = [m for m in MODEL_STYLES if m in models]
    return known + sorted(m for m in models if m not in MODEL_STYLES)


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

class ForecastStore:
    def __init__(self, url, org, token, bucket, location, verify_ssl=True):
        self.url = url.rstrip("/")
        self.org = org
        self.token = token
        self.bucket = bucket
        self.location = location
        self.verify_ssl = verify_ssl

    def _query(self, flux):
        headers = {"Authorization": f"Token {self.token}", "Content-Type": "application/vnd.flux",
                   "Accept": "application/csv"}
        org_param = {"orgID": self.org} if re.fullmatch(r"[0-9a-fA-F]{16}", self.org) else {"org": self.org}
        try:
            response = requests.post(f"{self.url}/api/v2/query", params=org_param, headers=headers,
                                     data=flux.encode("utf-8"), timeout=REQUEST_TIMEOUT, verify=self.verify_ssl)
        except requests.RequestException as e:
            raise InfluxQueryError(f"Could not reach InfluxDB at {self.url}: {e}")
        if not 200 <= response.status_code < 300:
            raise InfluxQueryError(f"InfluxDB query failed with status {response.status_code}: {response.text[:300]}")
        return _parse_annotated_csv(response.text)

    def load(self, field, window_days):
        """Returns (forecasts, reference):
        forecasts[model][target_date][lead_h] = value, reference[target_date] = value."""
        bucket, loc = _flux_escape(self.bucket), _flux_escape(self.location)
        # Forecast points are stamped with the day they are FOR, which is in the future: stop must reach ahead.
        flux_forecast = f'''from(bucket: "{bucket}")
  |> range(start: -{window_days}d, stop: 20d)
  |> filter(fn: (r) => r._measurement == "forecast" and r.location == "{loc}" and r._field == "{field}")
  |> keep(columns: ["_time", "model", "lead_h", "_value"])
'''
        flux_reference = f'''from(bucket: "{bucket}")
  |> range(start: -{window_days + 2}d, stop: 2d)
  |> filter(fn: (r) => r._measurement == "actual" and r.model == "reference" and r.location == "{loc}" and r._field == "{field}")
  |> keep(columns: ["_time", "_value"])
'''
        forecasts = defaultdict(lambda: defaultdict(dict))
        for row in self._query(flux_forecast):
            ts, model = _parse_influx_time(row.get("_time")), row.get("model")
            try:
                value, lead_h = float(row["_value"]), int(row["lead_h"])
            except (KeyError, TypeError, ValueError):
                continue
            if ts and model:
                forecasts[model][ts.date()][lead_h] = value
        reference = {}
        for row in self._query(flux_reference):
            ts = _parse_influx_time(row.get("_time"))
            try:
                value = float(row["_value"])
            except (KeyError, TypeError, ValueError):
                continue
            if ts:
                reference[ts.date()] = value
        return forecasts, reference


# ---------------------------------------------------------------------------
# Analysis (pure functions)
# ---------------------------------------------------------------------------

def lead_day(lead_h):
    return max(0, int(round(lead_h / 24.0)))


def error_stats(forecasts, reference):
    """{model: {lead_day: (mean_abs_error, n)}} comparing every stored forecast with the reference."""
    stats = {}
    for model, by_date in forecasts.items():
        sums = defaultdict(lambda: [0.0, 0])
        for target, by_lead in by_date.items():
            if target not in reference:
                continue
            for lead_h, value in by_lead.items():
                acc = sums[lead_day(lead_h)]
                acc[0] += abs(value - reference[target])
                acc[1] += 1
        if sums:
            stats[model] = {d: (total / n, n) for d, (total, n) in sums.items()}
    return stats


def pick_cone_target(forecasts, reference, today, mode="auto", days_ahead=DEFAULT_CONE_DAYS_AHEAD):
    """Choose the day whose forecast history is drawn as a cone. Returns (date, completed)."""
    if mode != "upcoming":
        for target in sorted(reference, reverse=True):
            if target >= today:
                continue
            spans = [max(by_date[target]) - min(by_date[target])
                     for by_date in forecasts.values() if len(by_date.get(target, {})) > 1]
            if len([s for s in spans if s >= CONE_MIN_SPAN_DAYS * 24]) >= CONE_MIN_MODELS:
                return target, True
    return today + timedelta(days=days_ahead), False


def cone_series(forecasts, target):
    """{model: [(lead_h, value), ...]} sorted by lead, for one target day."""
    return {model: sorted(by_date[target].items()) for model, by_date in forecasts.items() if target in by_date}


def cone_band(series):
    """[(lead_bucket_h, lo, hi)] min/max across models, in coarse lead buckets (needs 2+ models)."""
    buckets = defaultdict(list)
    for points in series.values():
        for lead_h, value in points:
            buckets[round(lead_h / BAND_BUCKET_HOURS) * BAND_BUCKET_HOURS].append(value)
    return [(lead, min(v), max(v)) for lead, v in sorted(buckets.items()) if len(v) >= 2]


# ---------------------------------------------------------------------------
# Drawing helpers
# ---------------------------------------------------------------------------

def _nice_step(span, target_ticks):
    raw = span / max(1, target_ticks)
    magnitude = 10 ** math.floor(math.log10(raw))
    for factor in (1, 2, 2.5, 5, 10):
        if raw <= factor * magnitude * 1.0001:
            return factor * magnitude
    return 10 * magnitude


def _y_axis(lo, hi, target_ticks=5):
    if hi - lo < 1e-9:
        lo, hi = lo - 1, hi + 1
    step = _nice_step(hi - lo, target_ticks)
    start = int(lo // step) * step
    ticks = []
    value = start
    while value <= hi + step:
        ticks.append(value)
        value += step
        if len(ticks) > 30:
            break
    return ticks[0], ticks[-1], ticks


def _draw_polyline(draw, points, color, width, dash):
    if len(points) < 2:
        if points:
            x, y = points[0]
            draw.ellipse([x - width * 2, y - width * 2, x + width * 2, y + width * 2], fill=color)
        return
    if not dash:
        draw.line(points, fill=color, width=width, joint="curve")
        return
    on, off = dash
    drawing, remaining = True, on
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        seg = ((x1 - x0) ** 2 + (y1 - y0) ** 2) ** 0.5
        if seg == 0:
            continue
        pos = 0.0
        while pos < seg:
            step = min(remaining, seg - pos)
            if drawing:
                a, b = pos / seg, (pos + step) / seg
                draw.line([(x0 + (x1 - x0) * a, y0 + (y1 - y0) * a), (x0 + (x1 - x0) * b, y0 + (y1 - y0) * b)],
                          fill=color, width=width)
            pos += step
            remaining -= step
            if remaining <= 0:
                drawing = not drawing
                remaining = on if drawing else off


def _fmt(value, decimals):
    text = f"{value:.{decimals}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


class ForecastAccuracy(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params['api_key'] = {
            "required": True,
            "service": "InfluxDB (Forecast Log)",
            "expected_key": "INFLUXDB_FORECAST_TOKEN",
        }
        return template_params

    def generate_image(self, settings, device_config):
        token = device_config.load_env_key("INFLUXDB_FORECAST_TOKEN")
        if not token:
            raise RuntimeError("InfluxDB token not configured (set INFLUXDB_FORECAST_TOKEN in .env).")

        store = ForecastStore(
            (settings.get("influxUrl") or "").strip() or DEFAULT_INFLUX_URL,
            (settings.get("influxOrg") or "").strip() or DEFAULT_INFLUX_ORG,
            token,
            (settings.get("influxBucket") or "").strip() or DEFAULT_BUCKET,
            (settings.get("location") or "").strip() or DEFAULT_LOCATION,
            str(settings.get("skipTlsVerify", "")).lower() != "true",
        )
        metric = settings.get("metric") if settings.get("metric") in METRICS else "tmax"
        imperial = settings.get("units") == "imperial"
        window_days = self._int(settings.get("windowDays"), DEFAULT_WINDOW_DAYS, 7, 365)
        cone_mode = "upcoming" if settings.get("coneMode") == "upcoming" else "auto"
        cone_ahead = self._int(settings.get("coneDaysAhead"), DEFAULT_CONE_DAYS_AHEAD, 1, 14)
        title = (settings.get("title") or "").strip() or DEFAULT_TITLE

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]
        tz = pytz.timezone(device_config.get_config("timezone", default="America/New_York"))
        time_format = device_config.get_config("time_format", default="12h")
        now_dt = datetime.now(tz)

        try:
            forecasts, reference = store.load(METRICS[metric][1], window_days)
        except InfluxQueryError as e:
            logger.error(f"Failed to query InfluxDB: {e}")
            raise RuntimeError("Failed to retrieve forecast data from InfluxDB, please check logs.")

        return self.render(dimensions, title, metric, imperial, forecasts, reference, now_dt, time_format,
                           cone_mode, cone_ahead, window_days)

    @staticmethod
    def _int(raw, default, lo, hi):
        try:
            return max(lo, min(hi, int(raw)))
        except (TypeError, ValueError):
            return default

    # -- rendering -----------------------------------------------------------

    def render(self, dimensions, title, metric, imperial, forecasts, reference, now_dt, time_format,
               cone_mode="auto", cone_ahead=DEFAULT_CONE_DAYS_AHEAD, window_days=DEFAULT_WINDOW_DAYS):
        width, height = dimensions
        fonts = {
            "title": get_font("Jost", round(height * 0.040), "bold"),
            "head": get_font("Jost", round(height * 0.024), "bold"),
            "label": get_font("Jost", round(height * 0.020), "normal"),
            "labelb": get_font("Jost", round(height * 0.020), "bold"),
            "small": get_font("Jost", round(height * 0.016), "normal"),
            "cell": get_font("Jost", round(height * 0.018), "normal"),
            "cellb": get_font("Jost", round(height * 0.018), "bold"),
        }
        image = Image.new("RGB", dimensions, COLOR_WHITE)
        draw = ImageDraw.Draw(image)
        metric_title, _field, unit_metric, unit_imperial = METRICS[metric]
        unit = unit_imperial if imperial else unit_metric

        conv = self._converter(metric, imperial)
        forecasts = {m: {d: {lead: conv(v) for lead, v in leads.items()} for d, leads in by.items()}
                     for m, by in forecasts.items()}
        reference = {d: conv(v) for d, v in reference.items()}

        title_h = round(height * 0.075)
        draw.text((round(width * 0.012), title_h / 2), title, font=fonts["title"], fill=COLOR_BLACK, anchor="lm")
        stamp = now_dt.strftime("%Y-%m-%d %H:%M") if time_format == "24h" else now_dt.strftime("%Y-%m-%d %I:%M %p")
        draw.text((width - round(width * 0.012), title_h / 2), f"{metric_title} - updated {stamp}",
                  font=fonts["small"], fill=COLOR_BLACK, anchor="rm")
        draw.line([(0, title_h), (width, title_h)], fill=COLOR_BLACK, width=3)

        if not forecasts:
            draw.text((width / 2, height / 2), "No forecasts logged yet - check back after the next model run.",
                      font=fonts["head"], fill=COLOR_BLUE, anchor="mm")
            return image

        margin = round(width * 0.012)
        left_w = round(width * 0.60)
        body_top, body_bottom = title_h + margin, height - margin
        body_h = body_bottom - body_top
        cone_box = (margin, body_top, left_w - margin // 2, body_top + round(body_h * 0.52))
        error_box = (margin, cone_box[3] + margin, left_w - margin // 2, body_bottom)
        table_box = (left_w + margin // 2, body_top, width - margin, body_bottom)

        stats = error_stats(forecasts, reference)
        today = now_dt.date()
        target, completed = pick_cone_target(forecasts, reference, today, cone_mode, cone_ahead)

        self._draw_cone(draw, cone_box, fonts, forecasts, reference, target, completed, unit, metric)
        self._draw_error_chart(draw, error_box, fonts, stats, unit, metric)
        self._draw_table(draw, table_box, fonts, forecasts, stats, unit, metric, window_days)
        return image

    @staticmethod
    def _converter(metric, imperial):
        if not imperial:
            return lambda v: v
        if metric in ("tmax", "tmin"):
            return lambda v: v * 9 / 5 + 32
        if metric == "precip":
            return lambda v: v / 25.4
        return lambda v: v * 0.621371

    @staticmethod
    def _decimals(metric, imperial_or_unit):
        return 2 if imperial_or_unit == "in" else 1

    def _panel(self, draw, box, fonts, heading, note=None):
        x0, y0, x1, y1 = box
        draw.rectangle(box, outline=COLOR_BLACK, width=2)
        pad = round((x1 - x0) * 0.015)
        draw.text((x0 + pad, y0 + pad * 0.6), heading, font=fonts["head"], fill=COLOR_BLACK, anchor="la")
        if note:
            draw.text((x1 - pad, y0 + pad * 0.6 + 4), note, font=fonts["small"], fill=COLOR_BLACK, anchor="ra")
        return pad

    def _plot_area(self, box, fonts, pad):
        x0, y0, x1, y1 = box
        top = y0 + pad + fonts["head"].size + 14
        return (x0 + pad + 52, top, x1 - pad - 34, y1 - pad - fonts["small"].size * 2 - 30)

    def _draw_axes(self, draw, area, fonts, y_ticks, x_ticks, x_to_px, y_to_px, decimals):
        ax0, ay0, ax1, ay1 = area
        for value in y_ticks:
            y = y_to_px(value)
            draw.line([(ax0, y), (ax1, y)], fill=COLOR_GRID, width=1)
            draw.text((ax0 - 8, y), _fmt(value, decimals), font=fonts["small"], fill=COLOR_BLACK, anchor="rm")
        for value, label in x_ticks:
            x = x_to_px(value)
            draw.line([(x, ay1), (x, ay1 + 5)], fill=COLOR_BLACK, width=2)
            draw.text((x, ay1 + 8), label, font=fonts["small"], fill=COLOR_BLACK, anchor="ma")
        draw.line([(ax0, ay0), (ax0, ay1), (ax1, ay1)], fill=COLOR_BLACK, width=2)

    def _draw_cone(self, draw, box, fonts, forecasts, reference, target, completed, unit, metric):
        series = cone_series(forecasts, target)
        day_label = f"{target.strftime('%a %b')} {target.day}"
        heading = f"How the forecast for {day_label} changed"
        decimals = self._decimals(metric, unit)
        if completed and target in reference:
            note = f"actual: {_fmt(reference[target], decimals)} {unit} (black dot)"
        else:
            note = "still forecast - outcome pending"
        pad = self._panel(draw, box, fonts, heading, note)
        area = self._plot_area(box, fonts, pad)
        ax0, ay0, ax1, ay1 = area
        if not series:
            draw.text(((ax0 + ax1) / 2, (ay0 + ay1) / 2), "No forecasts for this day yet", font=fonts["label"],
                      fill=COLOR_BLUE, anchor="mm")
            return

        max_lead_days = max(max(lead for lead, _ in pts) for pts in series.values()) / 24.0
        x_max = max(5.0, min(16.5, max_lead_days + 0.5))
        values = [v for pts in series.values() for _, v in pts]
        if completed and target in reference:
            values.append(reference[target])
        decimals = self._decimals(metric, unit)
        y_lo, y_hi, y_ticks = _y_axis(0 if metric in ("precip", "wind") else min(values), max(values))

        def x_to_px(lead_days):  # reversed: the farther the lead, the further left
            return ax1 - (lead_days / x_max) * (ax1 - ax0)

        def y_to_px(value):
            return ay1 - (value - y_lo) / (y_hi - y_lo) * (ay1 - ay0)

        step = 1 if x_max <= 8 else 2
        x_ticks = [(d, f"{d}d") for d in range(0, int(x_max) + 1, step)]
        x_ticks = [(d, "target" if d == 0 else label) for d, label in x_ticks]
        self._draw_axes(draw, area, fonts, y_ticks, x_ticks, x_to_px, y_to_px, decimals)

        band = cone_band(series)
        if len(band) >= 2:
            upper = [(x_to_px(lead / 24.0), y_to_px(hi)) for lead, _lo, hi in band]
            lower = [(x_to_px(lead / 24.0), y_to_px(lo)) for lead, lo, _hi in band]
            draw.polygon(upper + lower[::-1], fill=COLOR_BAND)
            self._redraw_grid(draw, area, y_ticks, y_to_px)

        for model in model_order(series):
            _label, color, dash, lw = _style(model)
            pts = [(x_to_px(lead / 24.0), y_to_px(v)) for lead, v in series[model] if lead / 24.0 <= x_max]
            _draw_polyline(draw, pts, color, 2 + lw, dash)

        if completed and target in reference:
            ref_y = y_to_px(reference[target])
            _draw_polyline(draw, [(ax0, ref_y), (ax1, ref_y)], COLOR_BLACK, 1, (4, 6))
            r = 9
            draw.ellipse([ax1 - r, ref_y - r, ax1 + r, ref_y + r], fill=COLOR_BLACK, outline=COLOR_WHITE, width=2)
        draw.text(((ax0 + ax1) / 2, box[3] - pad * 0.5), "days before the target day", font=fonts["small"],
                  fill=COLOR_BLACK, anchor="md")

    @staticmethod
    def _redraw_grid(draw, area, y_ticks, y_to_px):
        ax0, _ay0, ax1, _ay1 = area
        for value in y_ticks:
            y = y_to_px(value)
            draw.line([(ax0, y), (ax1, y)], fill=COLOR_GRID, width=1)

    def _draw_error_chart(self, draw, box, fonts, stats, unit, metric):
        pad = self._panel(draw, box, fonts, f"Average error by forecast distance ({unit})", "lower is better")
        area = self._plot_area(box, fonts, pad)
        ax0, ay0, ax1, ay1 = area
        series = {m: sorted((d, mae) for d, (mae, n) in by.items() if n >= MIN_SAMPLES) for m, by in stats.items()}
        series = {m: pts for m, pts in series.items() if pts}
        if not series:
            draw.text(((ax0 + ax1) / 2, (ay0 + ay1) / 2),
                      "Accuracy appears once forecast days have passed", font=fonts["label"],
                      fill=COLOR_BLUE, anchor="mm")
            return
        x_max = max(max(d for d, _ in pts) for pts in series.values())
        x_max = max(3, x_max)
        top = max(mae for pts in series.values() for _, mae in pts)
        y_lo, y_hi, y_ticks = _y_axis(0, top * 1.05, 4)
        decimals = self._decimals(metric, unit)

        def x_to_px(day):
            return ax0 + (day / x_max) * (ax1 - ax0 - 6) + 3

        def y_to_px(value):
            return ay1 - (value - y_lo) / (y_hi - y_lo) * (ay1 - ay0)

        step = 1 if x_max <= 8 else 2
        x_ticks = [(d, f"{d}d") for d in range(0, x_max + 1, step)]
        self._draw_axes(draw, area, fonts, y_ticks, x_ticks, x_to_px, y_to_px, decimals)
        for model in model_order(series):
            _label, color, dash, lw = _style(model)
            _draw_polyline(draw, [(x_to_px(d), y_to_px(mae)) for d, mae in series[model]], color, 2 + lw, dash)
        draw.text(((ax0 + ax1) / 2, box[3] - pad * 0.5), "days ahead the forecast was made", font=fonts["small"],
                  fill=COLOR_BLACK, anchor="md")

    def _draw_table(self, draw, box, fonts, forecasts, stats, unit, metric, window_days):
        pad = self._panel(draw, box, fonts, "Scoreboard", f"avg error, {unit}")
        x0, y0, x1, y1 = box
        models = model_order(forecasts.keys())
        label_w = round((x1 - x0) * 0.30)
        col_w = (x1 - x0 - 2 * pad - label_w) / len(TABLE_LEADS)
        row_top = y0 + pad + fonts["head"].size + 30
        header_h = fonts["labelb"].size + 14
        for i, lead in enumerate(TABLE_LEADS):
            draw.text((x0 + pad + label_w + col_w * (i + 0.5), row_top + header_h / 2), f"{lead}d",
                      font=fonts["labelb"], fill=COLOR_BLACK, anchor="mm")
        draw.line([(x0 + pad, row_top + header_h), (x1 - pad, row_top + header_h)], fill=COLOR_BLACK, width=2)

        footer_h = fonts["small"].size * 4 + 36
        avail = y1 - pad - footer_h - (row_top + header_h)
        row_h = min(round(fonts["label"].size * 3.0), avail / max(1, len(models)))
        best = {}
        for lead in TABLE_LEADS:
            cells = [(stats[m][lead][0], m) for m in models if m in stats and lead in stats[m]
                     and stats[m][lead][1] >= MIN_SAMPLES]
            if len(cells) >= 2:
                best[lead] = min(cells)[1]
        decimals = self._decimals(metric, unit)

        y = row_top + header_h
        for model in models:
            label, color, dash, lw = _style(model)
            cy = y + row_h / 2
            _draw_polyline(draw, [(x0 + pad, cy), (x0 + pad + 46, cy)], color, 3 + lw, dash)
            draw.text((x0 + pad + 56, cy), label, font=fonts["label"], fill=COLOR_BLACK, anchor="lm")
            for i, lead in enumerate(TABLE_LEADS):
                cx = x0 + pad + label_w + col_w * (i + 0.5)
                cell = stats.get(model, {}).get(lead)
                if cell and cell[1] >= MIN_SAMPLES:
                    is_best = best.get(lead) == model
                    if is_best:
                        draw.rectangle([cx - col_w * 0.48, y + 3, cx + col_w * 0.48, y + row_h - 3], fill=COLOR_GREEN)
                    draw.text((cx, cy), f"{cell[0]:.{decimals}f}", font=fonts["cellb" if is_best else "cell"],
                              fill=COLOR_WHITE if is_best else COLOR_BLACK, anchor="mm")
                else:
                    draw.text((cx, cy), "-", font=fonts["cell"], fill=COLOR_GRID, anchor="mm")
            y += row_h

        total = sum(n for by in stats.values() for _, n in by.values())
        lines = [
            "Green = best at that distance.",
            f"{total} comparisons over the last {window_days} days.",
            "Truth = Open-Meteo reanalysis.",
            "'-' = out of range, or too few samples.",
        ]
        fy = y1 - pad - footer_h + 8
        for line in lines:
            draw.text((x0 + pad, fy), line, font=fonts["small"], fill=COLOR_BLACK, anchor="la")
            fy += fonts["small"].size + 6
