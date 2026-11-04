#!/usr/bin/env python3
"""forecastlog - records every new weather forecast in InfluxDB, per model.

Purpose: study how a forecast for a given day changes as that day approaches, how accurate
it turns out to be at each lead time (a "cone of uncertainty"), and which model is best.

It does NOT poll forecasts on a timer. Every `poll_minutes` it reads Open-Meteo's tiny
per-model run metadata; only when a model has published a new run does it download that
model's forecast for each configured location and write it. Models run on their own
schedules (most every 6 h; HRRR every hour).

Beyond Open-Meteo's models, three more sources are logged under the same schema (model tags
`met_norway`, `eccc` = Environment Canada's official forecast, `owm` = OpenWeatherMap). They have
no model-run metadata, so each is polled every `poll_minutes` and written when its own
"updated" stamp changes (OpenWeatherMap: every `interval_minutes`). Their daily values are
aggregated into local calendar days, and days they only partly cover are skipped.

Schema (bucket `forecast`, retained forever - tiny):

  forecast,location=<name>,model=<model>,lead_h=<hours>   tmax, tmin, precip, wind, wdir, code, run
      timestamp = the forecast's TARGET day, anchored at 12:00 UTC (date only; tz-independent)
      lead_h    = hours from the model run's initialisation to that anchor (multiple of 6 for
                  6-hourly models; small or negative for "today" forecasts made late in the day)
      run       = the model run's initialisation time (epoch seconds)
      A run's long-range days can be filled in late; re-writing the same run just completes it.
  actual,location=<name>,model=<model|reference>          same fields, no run
      timestamp = the day, same anchor. model=<model> is that model's own analysis of days
                  that have passed (rewritten each run). model=reference is the common truth
                  from Open-Meteo's archive (reanalysis), the fair yardstick across models.

Units are always metric: tmax/tmin degC, precip mm, wind km/h (daily max), wdir degrees.

Cone for target day D:  forecast, range D..D, grouped by model and lead_h.
Error vs lead:          forecast(model, lead_h) - actual(model=reference), same timestamp.

Only the Python standard library is used.
"""
import argparse
import configparser
import json
import logging
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger("forecastlog")

DEFAULT_CONFIG_PATH = "/etc/forecastlog/forecastlog.conf"
DEFAULT_STATE_PATH = "/var/lib/forecastlog/state.json"

DEFAULTS = {
    "influx": {"url": "http://localhost:8086", "org": "none", "token": "", "bucket": "forecast"},
    "open_meteo": {
        "poll_minutes": "10",
        "forecast_days": "16",
        "past_days": "3",
        "reference_days": "14",  # how far back to (re)write the archive "reference" truth
    },
    "sources": {"met_norway": "false", "eccc": "false", "owm": "false", "poll_minutes": "30"},
    "met_norway": {"user_agent": "forecastlog/1.0 (personal weather logging project)"},
    "owm": {"api_key": "", "interval_minutes": "360"},
}

DAILY_VARS = ("weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,"
              "wind_speed_10m_max,wind_direction_10m_dominant")
FORECAST_URL = (
    "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&daily=" + DAILY_VARS +
    "&timezone=auto&models={models}&past_days={past_days}&forecast_days={forecast_days}"
)
ARCHIVE_URL = (
    "https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&daily=" + DAILY_VARS +
    "&timezone=auto&start_date={start}&end_date={end}"
)
REFERENCE_REFRESH_SECONDS = 6 * 3600
META_URL = "https://api.open-meteo.com/data/{model}/static/meta.json"
MET_URL = "https://api.met.no/weatherapi/locationforecast/2.0/complete?lat={lat:.4f}&lon={lon:.4f}"
ECCC_URL = "https://api.weather.gc.ca/collections/citypageweather-realtime/items/{site}?f=json&lang=en"
OWM_URL = ("https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&units=metric"
           "&exclude=minutely,hourly,current,alerts&appid={key}")
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
# Open-Meteo response key -> InfluxDB field name, field kind (f = float, i = integer)
DAILY_FIELDS = [
    ("temperature_2m_max", "tmax", "f"),
    ("temperature_2m_min", "tmin", "f"),
    ("precipitation_sum", "precip", "f"),
    ("wind_speed_10m_max", "wind", "f"),
    ("wind_direction_10m_dominant", "wdir", "f"),
    ("weather_code", "code", "i"),
]


def load_config(path):
    parser = configparser.ConfigParser()
    parser.read_dict(DEFAULTS)
    if not parser.read(path):
        raise SystemExit(f"Config file not found: {path}")
    for section in ("locations", "models"):
        if not parser.has_section(section) or not parser.items(section):
            raise SystemExit(f"No [{section}] in {path}")
    return parser


def parse_locations(parser):
    """name -> (lat, lon, timezone name or None). Format: 'latitude, longitude[, Area/City]'."""
    locations = {}
    for name, value in parser.items("locations"):
        parts = [p.strip() for p in value.split(",")]
        try:
            lat, lon = float(parts[0]), float(parts[1])
        except (ValueError, IndexError):
            raise SystemExit(f"[locations] {name}: expected 'latitude, longitude[, timezone]', got {value!r}")
        locations[name] = (lat, lon, parts[2] if len(parts) > 2 and parts[2] else None)
    return locations


def http_get_json(url, timeout=30, headers=None):
    request = urllib.request.Request(url, headers=dict({"User-Agent": "forecastlog/1.0"}, **(headers or {})))
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def read_model_run(meta_name):
    """(availability_time, initialisation_time) of a model's newest complete run, epoch seconds."""
    meta = http_get_json(META_URL.format(model=meta_name), timeout=15)
    return int(meta["last_run_availability_time"]), int(meta["last_run_initialisation_time"])


def _tag(value):
    return str(value).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")


def _fields(row):
    parts = []
    for name, value, kind in row:
        if value is None:
            continue
        parts.append(f"{name}={int(value)}i" if kind == "i" else f"{name}={float(value)}")
    return ",".join(parts)


def _anchor(day):
    return int(datetime(day.year, day.month, day.day, 12, tzinfo=timezone.utc).timestamp())


def _series(daily, key, model):
    """Open-Meteo suffixes keys with the model name when several models are requested."""
    return daily.get(f"{key}_{model}", daily.get(key, []))


def build_lines(location, model, data, init_ts, now_utc):
    """Line-protocol lines for one location + model from an Open-Meteo response.
    Days before today become `actual` (the model's analysis); today onwards are `forecast`."""
    daily = data.get("daily", {})
    series = {key: _series(daily, key, model) for key, _, _ in DAILY_FIELDS}
    today = (now_utc + timedelta(seconds=data.get("utc_offset_seconds", 0) or 0)).date()
    lines = []
    for i, day_str in enumerate(daily.get("time", [])):
        day = date.fromisoformat(day_str)
        anchor = _anchor(day)
        row = [(field, series[key][i] if i < len(series[key]) else None, kind) for key, field, kind in DAILY_FIELDS]
        if all(v is None for _, v, _ in row):
            continue
        if day >= today:
            lead_h = (anchor - init_ts) // 3600
            lines.append(
                f"forecast,location={_tag(location)},model={_tag(model)},lead_h={lead_h} "
                f"{_fields(row + [('run', init_ts, 'i')])} {anchor}"
            )
        else:
            lines.append(f"actual,location={_tag(location)},model={_tag(model)} {_fields(row)} {anchor}")
    return lines


def build_reference_lines(location, data):
    """`actual` lines with model=reference from the archive API (days without data are skipped)."""
    daily = data.get("daily", {})
    lines = []
    for i, day_str in enumerate(daily.get("time", [])):
        anchor = _anchor(date.fromisoformat(day_str))
        row = []
        for key, field, kind in DAILY_FIELDS:
            values = daily.get(key, [])
            row.append((field, values[i] if i < len(values) else None, kind))
        if all(v is None for _, v, _ in row):
            continue
        lines.append(f"actual,location={_tag(location)},model=reference {_fields(row)} {anchor}")
    return lines


# ---------------------------------------------------------------------------
# Extra sources (no model-run metadata): MET Norway, Environment Canada, OpenWeatherMap
# Each fetch returns (update_token, init_ts, {date: {tmax, tmin, precip, wind, wdir, code}}).
# ---------------------------------------------------------------------------

def _parse_z(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _dominant_direction(pairs):
    """Speed-weighted mean wind direction (degrees) from (speed, from_degrees) pairs."""
    x = sum(w * math.sin(math.radians(d)) for w, d in pairs)
    y = sum(w * math.cos(math.radians(d)) for w, d in pairs)
    if not pairs or (abs(x) < 1e-9 and abs(y) < 1e-9):
        return None
    return round(math.degrees(math.atan2(x, y)) % 360)


def met_daily(data, tz):
    """Aggregate MET Norway's hourly-then-6-hourly series into complete local days."""
    temps, winds = defaultdict(list), defaultdict(list)
    precip, covered = defaultdict(float), defaultdict(int)
    for entry in data["properties"]["timeseries"]:
        t = _parse_z(entry["time"])
        details = entry["data"]["instant"]["details"]
        day = t.astimezone(tz).date()
        if details.get("air_temperature") is not None:
            temps[day].append(details["air_temperature"])
        if details.get("wind_speed") is not None:
            winds[day].append((details["wind_speed"] * 3.6, details.get("wind_from_direction")))
        n1, n6 = entry["data"].get("next_1_hours"), entry["data"].get("next_6_hours")
        amount1 = (n1 or {}).get("details", {}).get("precipitation_amount")
        if amount1 is not None:
            precip[day] += amount1
            covered[day] += 1
        elif n6 and n6.get("details", {}).get("precipitation_amount") is not None:
            for k in range(6):  # spread a 6 h block evenly over its hours
                h_day = (t + timedelta(hours=k)).astimezone(tz).date()
                precip[h_day] += n6["details"]["precipitation_amount"] / 6
                covered[h_day] += 1
        if n6:
            block_day = (t + timedelta(hours=6) - timedelta(seconds=1)).astimezone(tz).date()
            if block_day == day:  # blocks straddling midnight can't be attributed to one day
                for key in ("air_temperature_max", "air_temperature_min"):
                    if n6.get("details", {}).get(key) is not None:
                        temps[day].append(n6["details"][key])
    out = {}
    for day in sorted(temps):
        if covered[day] < 23 or not temps[day]:
            continue
        speeds = [w for w, _ in winds[day]]
        out[day] = {
            "tmax": max(temps[day]), "tmin": min(temps[day]), "precip": round(precip[day], 2),
            "wind": round(max(speeds), 1) if speeds else None,
            "wdir": _dominant_direction([(w, d) for w, d in winds[day] if d is not None]), "code": None,
        }
    return out


def fetch_met_norway(lat, lon, tz, parser, prior):
    """Returns None when unchanged (HTTP 304), else (token, init_ts, daily, state_update)."""
    headers = {"User-Agent": parser.get("met_norway", "user_agent")}
    if prior.get("last_modified"):
        headers["If-Modified-Since"] = prior["last_modified"]
    request = urllib.request.Request(MET_URL.format(lat=lat, lon=lon), headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            last_modified = response.headers.get("Last-Modified")
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return None
        raise
    updated = data["properties"]["meta"]["updated_at"]
    return updated, int(_parse_z(updated).timestamp()), met_daily(data, tz), {"last_modified": last_modified}


def fetch_eccc(site, tz):
    """Environment Canada's city forecast: day periods give highs, night periods lows."""
    data = http_get_json(ECCC_URL.format(site=site), timeout=30)["properties"]
    stamp = data["forecastGroup"]["timestamp"]["en"]
    issued = _parse_z(stamp)
    issue_date = issued.astimezone(tz).date()
    daily = defaultdict(dict)
    precip = defaultdict(lambda: [0.0, set()])
    for period in data["forecastGroup"]["forecasts"]:
        name = period["period"]["textForecastName"]["en"].lower()
        weekday = period["period"]["value"]["en"].lower().split()[0]  # "Wednesday night" -> "wednesday"
        if weekday not in WEEKDAYS:
            continue
        day = next(issue_date + timedelta(days=k) for k in range(7)
                   if (issue_date + timedelta(days=k)).weekday() == WEEKDAYS.index(weekday))
        is_night = "night" in name or name == "tonight"
        for temp in period.get("temperatures", {}).get("temperature", []):
            value = temp["value"]["en"]
            if temp["class"]["en"] == "high" and not is_night:
                daily[day]["tmax"] = value
            elif temp["class"]["en"] == "low" and is_night:
                daily[day + timedelta(days=1)]["tmin"] = value  # the overnight low falls on the next calendar day
        amount = period.get("precipitation", {}).get("accumulation", {}).get("amount", {}).get("value", {}).get("en")
        precip[day][0] += amount or 0
        precip[day][1].add("night" if is_night else "day")
        winds = period.get("winds", {}).get("periods", [])
        speeds = [w["speed"]["value"]["en"] for w in winds if "speed" in w]
        if speeds and not is_night:
            daily[day]["wind"] = max(speeds)
            bearing = winds[0].get("bearing", {}).get("value", {}).get("en")
            if bearing is not None:
                daily[day]["wdir"] = bearing
    for day, (total, parts) in precip.items():
        if parts == {"day", "night"}:  # only when both halves of the calendar day were forecast
            daily[day]["precip"] = total
    out = {}
    for day, values in daily.items():
        if day >= issue_date and values:
            out[day] = {k: values.get(k) for k in ("tmax", "tmin", "precip", "wind", "wdir")}
            out[day]["code"] = None
    return stamp, int(issued.timestamp()), out


def fetch_owm(lat, lon, key):
    data = http_get_json(OWM_URL.format(lat=lat, lon=lon, key=key), timeout=30)
    offset = data.get("timezone_offset", 0) or 0
    out = {}
    for d in data.get("daily", []):
        day = (datetime.fromtimestamp(d["dt"], timezone.utc) + timedelta(seconds=offset)).date()
        out[day] = {
            "tmax": d["temp"]["max"], "tmin": d["temp"]["min"],
            "precip": round(d.get("rain", 0) + d.get("snow", 0), 2),
            "wind": round(d.get("wind_speed", 0) * 3.6, 1), "wdir": d.get("wind_deg"), "code": None,
        }
    now = int(time.time())
    return str(now // 3600), now // 3600 * 3600, out


def lines_from_daily(location, model, daily, init_ts, now_utc, tz):
    """Same schema as build_lines, for already-aggregated {date: values} (today onwards only)."""
    today = now_utc.astimezone(tz).date()
    lines = []
    for day in sorted(daily):
        if day < today:
            continue
        row = [(field, daily[day].get(field), kind) for _key, field, kind in DAILY_FIELDS]
        if all(v is None for _, v, _ in row):
            continue
        anchor = _anchor(day)
        lines.append(
            f"forecast,location={_tag(location)},model={_tag(model)},lead_h={(anchor - init_ts) // 3600} "
            f"{_fields(row + [('run', init_ts, 'i')])} {anchor}"
        )
    return lines


def collect_extra_sources(parser, saved, force, dry_run):
    """Poll MET Norway / Environment Canada / OpenWeatherMap per location. Returns the new state."""
    state = saved.get("sources", {})
    now = time.time()
    now_utc = datetime.now(timezone.utc)
    poll = int(parser.get("sources", "poll_minutes")) * 60
    lines, touched = [], []
    for name, (lat, lon, tz_name) in parse_locations(parser).items():
        if not tz_name:
            logger.warning("Location %s needs a timezone (third value) for the extra sources", name)
            continue
        tz = ZoneInfo(tz_name)
        jobs = []
        if parser.getboolean("sources", "met_norway"):
            jobs.append(("met_norway", poll, lambda prior: fetch_met_norway(lat, lon, tz, parser, prior)))
        if parser.getboolean("sources", "eccc") and parser.has_option("eccc_sites", name):
            site = parser.get("eccc_sites", name)
            jobs.append(("eccc", poll, lambda prior, site=site: fetch_eccc(site, tz) + ({},)))
        if parser.getboolean("sources", "owm") and parser.get("owm", "api_key"):
            key = parser.get("owm", "api_key")
            jobs.append(("owm", int(parser.get("owm", "interval_minutes")) * 60,
                         lambda prior, key=key: fetch_owm(lat, lon, key) + ({},)))
        for model, interval, fetch in jobs:
            slot = f"{model}:{name}"
            prior = state.get(slot, {})
            if not force and now - prior.get("checked_at", 0) < interval:
                continue
            try:
                result = fetch(prior)
            except Exception as e:  # one source failing must not stop the others
                logger.warning("%s fetch failed for %s: %s", model, name, e)
                continue
            entry = dict(prior, checked_at=now)
            if result is not None:
                token, init_ts, daily, extra = result
                entry.update(extra)
                if force or token != prior.get("token"):
                    new_lines = lines_from_daily(name, model, daily, init_ts, now_utc, tz)
                    lines += new_lines
                    entry["token"] = token
                    touched.append(f"{model} ({len(new_lines)})")
            state[slot] = entry
    if lines:
        if dry_run:
            print("\n".join(lines))
        else:
            influx_write(parser, lines)
        logger.info("Recorded %s", ", ".join(touched))
    return state


def influx_write(parser, lines):
    params = {
        "org": parser.get("influx", "org"),
        "bucket": parser.get("influx", "bucket"),
        "precision": "s",
    }
    url = parser.get("influx", "url").rstrip("/") + "/api/v2/write?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(
        url, data="\n".join(lines).encode("utf-8"), method="POST",
        headers={"Authorization": "Token " + parser.get("influx", "token"),
                 "Content-Type": "text/plain; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15):
            pass
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"InfluxDB write failed: HTTP {e.code} {e.read().decode('utf-8', 'replace')[:200]}")
    except (urllib.error.URLError, OSError) as e:
        raise RuntimeError(f"Could not reach InfluxDB: {e}")


def collect(parser, models, runs, dry_run=False, with_reference=True):
    """Download and store the forecast of `models` (a list) for every location.
    `runs` maps model -> init epoch. Returns the number of points written."""
    cfg = parser["open_meteo"]
    now = datetime.now(timezone.utc)
    lines = []
    for name, (lat, lon, _tz) in parse_locations(parser).items():
        data = http_get_json(FORECAST_URL.format(
            lat=lat, lon=lon, models=",".join(models), past_days=cfg["past_days"], forecast_days=cfg["forecast_days"]))
        for model in models:
            lines += build_lines(name, model, data, runs[model], now)
        if not with_reference:
            continue
        try:
            start = (now - timedelta(days=int(cfg["reference_days"]))).date().isoformat()
            end = (now - timedelta(days=1)).date().isoformat()
            lines += build_reference_lines(name, http_get_json(
                ARCHIVE_URL.format(lat=lat, lon=lon, start=start, end=end)))
        except Exception as e:  # the reference is a bonus; never lose the forecasts over it
            logger.warning("Reference (archive) fetch failed for %s: %s", name, e)
    if dry_run:
        print("\n".join(lines))
    else:
        influx_write(parser, lines)
    return len(lines)


def load_state(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def save_state(path, state):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp, path)
    except OSError as e:
        logger.warning("Could not save state %s: %s", path, e)


def run_once(parser, state_path, force=False, dry_run=False):
    """Collect every model/source that has a new run. Returns the Open-Meteo models collected."""
    saved = load_state(state_path)
    state = saved.get("runs", {})
    sources_state = saved.get("sources", {})
    # The archive truth changes slowly; refreshing it on every hourly HRRR run would be wasteful.
    with_reference = force or time.time() - saved.get("reference_at", 0) >= REFERENCE_REFRESH_SECONDS
    due, runs = [], {}
    for model, meta_name in parser.items("models"):
        try:
            availability, init_ts = read_model_run(meta_name)
        except Exception as e:  # one model's metadata failing must not block the others
            logger.warning("Could not read run metadata for %s: %s", model, e)
            continue
        runs[model] = init_ts
        if force or state.get(model, {}).get("availability") != availability:
            due.append((model, availability, init_ts))

    if any(parser.getboolean("sources", k) for k in ("met_norway", "eccc", "owm")):
        sources_state = collect_extra_sources(parser, saved, force, dry_run)

    models = [m for m, _, _ in due]
    if due:
        count = collect(parser, models, runs, dry_run=dry_run, with_reference=with_reference)
        logger.info("Recorded %s (%d points)", ", ".join(
            f"{m} {datetime.fromtimestamp(i, timezone.utc).strftime('%d %HZ')}" for m, _, i in due), count)
        for model, availability, init_ts in due:
            state[model] = {"availability": availability, "init": init_ts}
    if not dry_run:
        save_state(state_path, {
            "runs": state, "sources": sources_state,
            "reference_at": time.time() if (due and with_reference) else saved.get("reference_at", 0)})
    return models


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    ap.add_argument("--state", default=DEFAULT_STATE_PATH)
    ap.add_argument("--once", action="store_true", help="check once and exit instead of running as a daemon")
    ap.add_argument("--force", action="store_true", help="collect everything now even if already recorded")
    ap.add_argument("--dry-run", action="store_true", help="print the line protocol instead of writing it")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    while True:
        parser = load_config(args.config)  # re-read each pass so edits apply without a restart
        try:
            run_once(parser, args.state, force=args.force, dry_run=args.dry_run)
        except Exception as e:  # network or Influx hiccup: try again next pass
            logger.warning("Collection failed (will retry): %s", e)
            if args.once:
                return 1
        if args.once:
            return 0
        args.force = False
        time.sleep(max(1, int(parser.get("open_meteo", "poll_minutes"))) * 60)


if __name__ == "__main__":
    sys.exit(main())
