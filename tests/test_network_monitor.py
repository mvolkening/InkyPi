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


class TestSpeedCharts:
    def test_monthly_stats_newest_first_with_empty_month(self):
        import pytz
        from datetime import datetime
        tz = pytz.UTC
        ts = lambda y, m, d: int(datetime(y, m, d, 12, tzinfo=pytz.UTC).timestamp())
        tests = [
            (ts(2026, 10, 1), 500.0, 480.0, 10.0, None),
            (ts(2026, 10, 2), 300.0, 400.0, 20.0, None),
            (ts(2026, 8, 5), 450.0, 450.0, None, None),
        ]
        rows = NetworkMonitor._monthly_stats(tests, tz, ts(2026, 10, 20), 3, 500)
        assert [r[0] for r in rows] == ["Oct 26", "Sep 26", "Aug 26"]
        label, down, up, min_down, slow, latency, count = rows[0]
        assert (down, up, min_down, slow, latency, count) == (400.0, 440.0, 300.0, 50.0, 15.0, 2)
        assert rows[1][1:] == (None, None, None, None, None, 0)
        assert rows[2][5] is None  # no idle latency recorded

    def test_aggregate(self):
        assert NetworkMonitor._aggregate([], "average") is None
        assert NetworkMonitor._aggregate([1, 3], "average") == 2
        assert NetworkMonitor._aggregate([1, 3], "max") == 3
        assert NetworkMonitor._aggregate([1, 3], "min") == 1

    def test_speed_charts_render(self):
        import pytz
        from PIL import Image, ImageDraw
        from plugins.network_monitor.network_monitor import get_font
        tz = pytz.UTC
        tests = [(NOW - h * 3600, 400.0 + h % 50, 450.0, 12.0, None) for h in range(24 * 14)]
        image = Image.new("RGB", (800, 480), "white")
        draw = ImageDraw.Draw(image)
        fonts = {k: get_font("Jost", s, w) for k, s, w in (("label", 10, "bold"), ("small", 7, "normal"))}
        plugin = NetworkMonitor({"id": "network_monitor"})
        plugin._draw_hourly_speed(draw, (0, 0, 260, 200), tests, tz, fonts, 2, "average", 500)
        plugin._draw_weekday_speed(draw, (0, 200, 260, 400), tests, tz, fonts, 2, "monday", "average", 500)
        plugin._draw_stats_table(draw, (260, 0, 800, 150), tests, tz, NOW, fonts, 500)
        assert image.getbbox() is not None
