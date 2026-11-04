"""Run-aware cache for Open-Meteo forecast responses.

Open-Meteo's forecast only changes when its weather model produces a new run
(GFS: every 6 hours). Screens refresh far more often than that, so instead of
downloading the forecast on every refresh we ask a tiny metadata endpoint when
the model last finished a run and only re-download when that has advanced.

The cached response is also dropped when the local calendar day rolls over,
because the daily arrays are anchored to "today" (past_days / forecast_days).
"""
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from utils.app_utils import resolve_path

logger = logging.getLogger(__name__)

META_URL = "https://api.open-meteo.com/data/{model}/static/meta.json"
# Model whose runs drive the cache. GFS is what Open-Meteo's default ("best_match")
# uses for the long range over North America; change it for other regions.
DEFAULT_RUN_MODEL = "ncep_gfs025"
META_TTL_SECONDS = 600           # don't ask the metadata endpoint more often than this
MAX_AGE_SECONDS = 12 * 3600      # safety net: never trust a cached forecast longer than this
UNVERIFIED_MAX_AGE_SECONDS = 3600  # if the run time can't be checked, behave like an hourly cache

_meta_cache = {}  # model -> (checked_at, availability_time)


def _cache_dir():
    path = resolve_path("cache")
    os.makedirs(path, exist_ok=True)
    return path


def latest_run(model=DEFAULT_RUN_MODEL):
    """Epoch time at which the model's newest run became available, or None if unknown."""
    now = time.time()
    hit = _meta_cache.get(model)
    if hit and now - hit[0] < META_TTL_SECONDS:
        return hit[1]
    run = None
    try:
        response = requests.get(META_URL.format(model=model), timeout=10)
        response.raise_for_status()
        run = int(response.json()["last_run_availability_time"])
    except Exception as e:
        logger.warning(f"Could not read Open-Meteo model run time ({model}): {e}")
    _meta_cache[model] = (now, run)
    return run


def _local_day(data):
    offset = data.get("utc_offset_seconds", 0) or 0
    return (datetime.now(timezone.utc) + timedelta(seconds=offset)).date().isoformat()


def _load(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _save(path, entry):
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(entry, f)
        os.replace(tmp, path)
    except OSError as e:
        logger.warning(f"Could not write forecast cache {path}: {e}")


def _is_current(entry, run):
    data = entry.get("data")
    if not isinstance(data, dict):
        return False
    age = time.time() - entry.get("fetched_at", 0)
    if age > MAX_AGE_SECONDS or entry.get("day") != _local_day(data):
        return False
    if run is None:
        return age < UNVERIFIED_MAX_AGE_SECONDS
    return entry.get("run") == run


def fetch_open_meteo(url, run_model=DEFAULT_RUN_MODEL, timeout=30, max_age=None):
    """GET an Open-Meteo forecast URL, reusing the cached response while the model run is unchanged.

    With run_model=None the cache is purely time-based (valid for `max_age` seconds) - for data
    that has no model-run metadata, such as air quality.

    Returns (json, refreshed) where `refreshed` is True when a new download happened.
    Falls back to a stale cached response if the download fails.
    """
    path = os.path.join(_cache_dir(), f"open_meteo_{hashlib.sha1(url.encode('utf-8')).hexdigest()[:16]}.json")
    cached = _load(path)
    run = latest_run(run_model) if run_model else None
    if run_model:
        usable = bool(cached) and _is_current(cached, run)
    else:
        usable = bool(cached) and isinstance(cached.get("data"), dict) and             time.time() - cached.get("fetched_at", 0) < (max_age or UNVERIFIED_MAX_AGE_SECONDS)
    if usable:
        logger.info("Open-Meteo data unchanged; using cached copy")
        return cached["data"], False

    try:
        response = requests.get(url, timeout=timeout)
        if not 200 <= response.status_code < 300:
            logger.error(f"Failed to retrieve Open-Meteo weather data: {response.content}")
            raise RuntimeError("Failed to retrieve Open-Meteo weather data.")
        data = response.json()
    except Exception:
        if cached and isinstance(cached.get("data"), dict):
            logger.warning("Open-Meteo request failed; using stale cached forecast")
            return cached["data"], False
        raise

    _save(path, {"run": run, "fetched_at": time.time(), "day": _local_day(data), "data": data})
    return data, True
