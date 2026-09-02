from datetime import datetime, timedelta

import pytz

from plugins.power_monitor.power_monitor import _counter_unit_scale

TZ = pytz.timezone("America/New_York")
START = datetime(2026, 7, 1)


def _daily_series(day_wh):
    """(ts, Wh) pairs at local midnight on consecutive days."""
    return [
        (int(TZ.localize(START + timedelta(days=i)).timestamp()), wh)
        for i, wh in enumerate(day_wh)
    ]


class TestCounterUnitScale:
    def test_matching_counter_returns_1(self):
        counter = _daily_series([40000.0, 42000.0, 38000.0])
        reference = _daily_series([41000.0, 42000.0, 39000.0])
        assert _counter_unit_scale(counter, reference, TZ) == 1.0

    def test_kwh_counter_read_as_wh_scales_up_1000x(self):
        # Meter reports kWh (e.g. 40.0 per day) but the plugin is set to Wh,
        # so the counter-derived values come out ~1000x below the watt integral.
        counter = _daily_series([40.0, 42.0, 38.0])
        reference = _daily_series([41000.0, 42000.0, 39000.0])
        assert _counter_unit_scale(counter, reference, TZ) == 1000.0

    def test_wh_counter_read_as_kwh_scales_down_1000x(self):
        counter = _daily_series([40000000.0, 42000000.0, 38000000.0])
        reference = _daily_series([41000.0, 42000.0, 39000.0])
        assert _counter_unit_scale(counter, reference, TZ) == 0.001

    def test_moderate_disagreement_trusts_counter(self):
        # Only an almost-exact 1000x mismatch is auto-corrected; anything else
        # keeps trusting the counter as configured.
        counter = _daily_series([40000.0, 42000.0, 38000.0])
        reference = _daily_series([200000.0, 210000.0, 190000.0])
        assert _counter_unit_scale(counter, reference, TZ) == 1.0

    def test_no_overlapping_days_returns_1(self):
        counter = _daily_series([40.0, 42.0])
        reference = [(int(TZ.localize(datetime(2026, 8, 1)).timestamp()), 41000.0)]
        assert _counter_unit_scale(counter, reference, TZ) == 1.0

    def test_empty_reference_returns_1(self):
        counter = _daily_series([40.0, 42.0])
        assert _counter_unit_scale(counter, [], TZ) == 1.0

    def test_zero_usage_returns_1(self):
        counter = _daily_series([0.0, 0.0])
        reference = _daily_series([0.0, 0.0])
        assert _counter_unit_scale(counter, reference, TZ) == 1.0
