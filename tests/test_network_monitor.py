from plugins.network_monitor.network_monitor import NetworkMonitor, _InfluxStore, _parse_influx_time

NOW = 1_800_000_000


def _store(outages, last_sample_ts):
    store = _InfluxStore("http://x", "org", "tok", "raw", "main", True)
    store._outages = [o if len(o) == 3 else (*o, "unknown") for o in outages]
    store.last_sample_ts = last_sample_ts
    return store


class TestGetOutages:
    def test_closed_outage_kept_when_overlapping_window(self):
        store = _store([(NOW - 500, NOW - 400)], NOW)
        assert store.get_outages(NOW - 1000, NOW, NOW) == [(NOW - 500, NOW - 400)]

    def test_outage_outside_window_dropped(self):
        store = _store([(NOW - 5000, NOW - 4000)], NOW)
        assert store.get_outages(NOW - 1000, NOW, NOW) == []

    def test_ongoing_outage_runs_to_now_while_daemon_reports(self):
        store = _store([(NOW - 300, None)], NOW - 2)
        assert store.get_outages(NOW - 1000, NOW, NOW) == [(NOW - 300, NOW)]

    def test_ongoing_outage_capped_at_last_sample_when_daemon_stale(self):
        store = _store([(NOW - 3000, None)], NOW - 600)
        assert store.get_outages(NOW - 5000, NOW, NOW) == [(NOW - 3000, NOW - 600)]


def test_parse_influx_time_handles_fractional_seconds():
    assert _parse_influx_time("2026-09-19T20:11:46.294Z") == _parse_influx_time("2026-09-19T20:11:46Z")
    assert _parse_influx_time("") is None


def test_median_spacing():
    tests = [(0, 1, 1, None, None), (3600, 1, 1, None, None), (7200, 1, 1, None, None), (20000, 1, 1, None, None)]
    assert NetworkMonitor._median_spacing(tests) == 3600
    assert NetworkMonitor._median_spacing(tests[:1]) == 0


class TestScopes:
    def test_outages_keep_scope_and_unknown_is_normalized(self):
        store = _store([(NOW - 500, NOW - 400, "isp"), (NOW - 300, NOW - 200, "local")], NOW)
        assert store.get_outages_scoped(NOW - 1000, NOW, NOW) == [
            (NOW - 500, NOW - 400, "isp"), (NOW - 300, NOW - 200, "local"),
        ]
        # the scope-less view still works for the sparklines/histogram
        assert store.get_outages(NOW - 1000, NOW, NOW) == [(NOW - 500, NOW - 400), (NOW - 300, NOW - 200)]

    def test_scope_counts_only_count_outages_starting_that_day(self):
        outages = [(100, 200, "isp"), (300, 400, "isp"), (500, 600, "local"), (5000, 5100, "isp")]
        assert NetworkMonitor._scope_counts(outages, 0, 1000) == {"isp": 2, "local": 1}

    def test_format_scope_counts_fixed_order_and_skips_zero(self):
        assert NetworkMonitor._format_scope_counts({"local": 1, "isp": 2}) == "I2 L1"
        assert NetworkMonitor._format_scope_counts({"unknown": 3}) == "?3"
        assert NetworkMonitor._format_scope_counts({}) == ""
