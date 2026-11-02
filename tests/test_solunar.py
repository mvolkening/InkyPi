from datetime import date, datetime, timezone

import pytz

from plugins.solunar.solunar import (
    Solunar, find_moon_events, moon_phase_info, moon_phase_name, phase_score, wind_compass,
)


def test_phase_matches_known_new_and_full_moon():
    # New moon 2026-11-09 ~21:02 UTC, full moon 2026-11-24 ~14:53 UTC
    age, illum = moon_phase_info(datetime(2026, 11, 9, 21, tzinfo=timezone.utc))
    assert illum < 1 and moon_phase_name(age) == "newmoon"
    age, illum = moon_phase_info(datetime(2026, 11, 24, 15, tzinfo=timezone.utc))
    assert illum > 99 and moon_phase_name(age) == "fullmoon"


def test_phase_score_peaks_at_new_and_full():
    assert phase_score(0.2) == 60 and phase_score(14.8) == 60 and phase_score(29.4) == 60
    assert phase_score(7.4) == 0


def test_events_are_roughly_twelve_hours_apart():
    start = datetime(2026, 11, 2, tzinfo=timezone.utc)
    end = datetime(2026, 11, 6, tzinfo=timezone.utc)
    events = find_moon_events(start, end, 43.65, -79.38)
    transits = [dt for dt, kind in events if kind in ("upper", "lower")]
    gaps = [(b - a).total_seconds() / 3600 for a, b in zip(transits, transits[1:])]
    assert gaps and all(12.0 < g < 12.8 for g in gaps)
    assert {"rise", "set"} <= {kind for _, kind in events}


def test_build_days_covers_28_days_and_flags_high_days():
    plugin = Solunar({"id": "solunar"})
    tz = pytz.timezone("America/Toronto")
    days = plugin.build_days(date(2026, 11, 1), date(2026, 11, 2), tz, 43.65, -79.38, {}, "12h")
    assert len(days) == 28
    assert days[0]["date"] == date(2026, 11, 1) and days[1]["is_today"]
    high = {d["date"].day for d in days if d["high"]}
    assert {9, 24} <= high  # new and full moon days
    assert 2 not in high


def test_wind_compass():
    assert wind_compass(0) == "N" and wind_compass(270) == "W" and wind_compass(359) == "N"
