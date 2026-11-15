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


def test_parse_influx_time_handles_nanoseconds():
    assert _parse_influx_time("2026-11-11T11:49:22.480996974Z") == _parse_influx_time("2026-11-11T11:49:22.480996Z")


def test_parse_influx_time_handles_fractional_seconds():
    assert _parse_influx_time("2026-09-19T20:11:46.294Z") == _parse_influx_time("2026-09-19T20:11:46Z")
    assert _parse_influx_time("") is None


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

    def test_stats_table_renders(self):
        import pytz
        from PIL import Image, ImageDraw
        from plugins.network_monitor.network_monitor import get_font
        tz = pytz.UTC
        tests = [(NOW - h * 3600, 400.0 + h % 50, 450.0, 12.0, None, 20.0, 30.0) for h in range(24 * 14)]
        image = Image.new("RGB", (800, 480), "white")
        draw = ImageDraw.Draw(image)
        fonts = {k: get_font("Jost", s, w) for k, s, w in (("label", 10, "bold"), ("small", 7, "normal"))}
        plugin = NetworkMonitor({"id": "network_monitor"})
        plugin._draw_stats_table(draw, (260, 0, 800, 150), tests, tz, NOW, fonts, 500)
        assert image.getbbox() is not None


class TestLineQuality:
    class _FakeStore:
        def __init__(self, quality):
            self._quality = quality

        def get_quality(self, since_ts, until_ts):
            return self._quality

    def _render(self, quality, tests):
        import pytz
        from PIL import Image, ImageDraw
        from plugins.network_monitor.network_monitor import get_font
        image = Image.new("RGB", (1000, 400), "white")
        draw = ImageDraw.Draw(image)
        fonts = {k: get_font("Jost", s, w) for k, s, w in (("label", 20, "bold"), ("small", 14, "normal"))}
        plugin = NetworkMonitor({"id": "network_monitor"})
        plugin._store = self._FakeStore(quality)
        plugin._draw_quality_panel(draw, (0, 0, 1000, 220), NOW, pytz.UTC, fonts, "12h")
        plugin._draw_stats_table(draw, (0, 220, 1000, 400), tests, pytz.UTC, NOW, fonts, 500, "12h")
        return image

    def test_renders_with_data_and_gaps(self):
        quality = {
            "loss": {0: 0.0, 5: 1.5, 23: 0.2}, "jitter": {h: 1.0 + h % 4 for h in range(24)},
            "dns_default": {h: 8.0 for h in range(0, 24, 2)}, "dns_public": {h: 12.0 for h in range(24)},
            "dns_failures": 3,
        }
        tests = [(NOW - 100, 480.0, 470.0, 12.0, None, 25.0, 40.0)]
        assert self._render(quality, tests).getbbox() is not None

    def test_renders_before_any_data_exists(self):
        empty = {"loss": {}, "jitter": {}, "dns_default": {}, "dns_public": {}, "dns_failures": 0}
        assert self._render(empty, []).getbbox() is not None

    def test_latest_reading_drops_loaded_latency_when_missing(self):
        plugin = NetworkMonitor({"id": "network_monitor"})
        import pytz
        full, short = plugin._latest_reading((NOW, 480.0, 470.0, 12.0, None, 25.0, 40.0), pytz.UTC, "12h")
        assert "Under load 25/40 ms" in full and "Under load" not in short
        assert plugin._latest_reading(None, pytz.UTC, "12h") == ["Latest: no speed tests yet"]


class TestDailyUsage:
    def test_minute_rows_parse_into_bytes_and_seconds(self):
        store = _InfluxStore("http://x", "org", "tok", "raw", "homeassistant", True)
        store._query = lambda flux: [
            {"_time": "2026-11-11T10:02:00Z", "_value": "600", "elapsed": "60", "entity_id": "exos_router_data_sent"},
            {"_time": "2026-11-11T10:01:00Z", "_value": "3000", "elapsed": "60", "entity_id": "exos_router_data_received"},
            {"_time": "2026-11-11T10:03:00Z", "_value": "", "elapsed": "60", "entity_id": "exos_router_data_received"},
        ]
        traffic = store.get_minute_traffic(NOW - 3600, NOW)
        assert traffic["down"] == [(_parse_influx_time("2026-11-11T10:01:00Z"), 3000.0, 60.0)]
        assert len(traffic["up"]) == 1

    def test_counter_wrap_is_counted_not_dropped(self):
        from plugins.network_monitor.network_monitor import _COUNTER_DIFF_FLUX, COUNTER_WRAP
        assert str(COUNTER_WRAP) in _COUNTER_DIFF_FLUX and "nonNegative" not in _COUNTER_DIFF_FLUX

    def test_minutes_become_per_day_totals_with_gaps(self):
        import pytz
        from datetime import datetime
        from plugins.network_monitor.network_monitor import summarize_usage
        now = int(datetime(2026, 11, 11, 15, tzinfo=pytz.UTC).timestamp())
        midnight = int(datetime(2026, 11, 11, tzinfo=pytz.UTC).timestamp())
        traffic = {"down": [(midnight, 1e9, 60), (midnight + 60, 2e9, 60)],  # first minute ends at midnight: yesterday
                   "up": [(midnight + 600, 5e8, 60)]}
        usage = summarize_usage(traffic, 3, pytz.UTC, now)
        assert len(usage["dates"]) == 3 and usage["dates"][-1] == datetime(2026, 11, 11).date()
        assert usage["down"] == [None, 1e9, 2e9] and usage["up"] == [None, None, 5e8]

    def test_speed_test_windows_are_left_out(self):
        from plugins.network_monitor.network_monitor import _drop_windows
        points = [(t, 1.0, 60) for t in (100, 160, 220, 280, 340)]
        assert [p[0] for p in _drop_windows(points, [(100, 220), (330, 400)])] == [100, 280]
        assert _drop_windows(points, []) == points

    def test_speedtest_windows_use_recorded_duration(self):
        from plugins.network_monitor.network_monitor import SPEEDTEST_DEFAULT_SECONDS, SPEEDTEST_TAIL_SECONDS
        store = _InfluxStore("http://x", "org", "tok", "raw", "netmon", True)
        replies = {"speedtest": [{"_time": "2026-11-11T10:00:00Z", "duration_s": "30.4"},
                                 {"_time": "2026-11-11T04:00:00Z"}],
                   "speedtest_run": [{"_time": "2026-11-11T16:00:00Z", "duration_s": "180"}]}
        store._pivoted = lambda measurement, start: replies[measurement]
        t = lambda s: _parse_influx_time(s)
        assert store.get_speedtest_windows(NOW) == [
            (t("2026-11-11T04:00:00Z"), t("2026-11-11T04:00:00Z") + SPEEDTEST_DEFAULT_SECONDS + SPEEDTEST_TAIL_SECONDS),
            (t("2026-11-11T10:00:00Z"), t("2026-11-11T10:00:00Z") + 30 + SPEEDTEST_TAIL_SECONDS),
            (t("2026-11-11T16:00:00Z"), t("2026-11-11T16:00:00Z") + 180 + SPEEDTEST_TAIL_SECONDS),
        ]

    def test_usage_panel_renders_with_and_without_data(self):
        from datetime import date, timedelta
        from PIL import Image, ImageDraw
        from plugins.network_monitor.network_monitor import get_font
        fonts = {k: get_font("Jost", s, w) for k, s, w in (("label", 20, "bold"), ("small", 14, "normal"))}
        plugin = NetworkMonitor({"id": "network_monitor"})
        dates = [date(2026, 11, 1) + timedelta(days=i) for i in range(14)]
        filled = {"dates": dates, "down": [(i + 1) * 2e9 for i in range(14)], "up": [i * 2e8 for i in range(14)]}
        empty = {"dates": dates, "down": [None] * 14, "up": [None] * 14}
        for usage in (filled, empty):
            image = Image.new("RGB", (700, 220), "white")
            plugin._draw_usage_panel(ImageDraw.Draw(image), (0, 0, 700, 220), usage, fonts)
            assert image.getbbox() is not None


class TestThroughputPanels:
    def _fonts(self):
        from plugins.network_monitor.network_monitor import get_font
        return {k: get_font("Jost", s, w) for k, s, w in
                (("label", 20, "bold"), ("small", 14, "normal"), ("big", 30, "bold"))}

    def test_bucket_series_means_and_peaks(self):
        means, peaks = NetworkMonitor._bucket_series([(0, 10.0), (60, 30.0), (1000, 5.0)], 0, 4, 900)
        assert means == [20.0, 5.0, None, None] and peaks == [30.0, 5.0, None, None]

    def test_nice_top_and_minutes(self):
        assert NetworkMonitor._nice_top(100.7) == 200 and NetworkMonitor._nice_top(7) == 10
        assert NetworkMonitor._fmt_minutes(45) == "45 min" and NetworkMonitor._fmt_minutes(125) == "2 h 05 min"

    def test_panels_render_with_and_without_data(self):
        from datetime import date, timedelta
        import pytz
        from PIL import Image, ImageDraw
        plugin = NetworkMonitor({"id": "network_monitor"})
        fonts = self._fonts()
        points = [(NOW - m * 60, 20.0 + (m * 7) % 90) for m in range(0, 1440)]
        limit = {"dates": [date(2026, 11, 1) + timedelta(days=i) for i in range(30)],
                 "down_min": [None] * 28 + [12.0, 3.0], "up_min": [None] * 28 + [0.0, 0.0],
                 "tiers": {50.0: 90.0, 80.0: 30.0}, "peak_down": 101.2, "peak_up": 22.0, "data_days": 2.1}
        full = {"series": {"down": points, "up": points[::3]}, "limit": limit}
        for traffic in (full, None):
            image = Image.new("RGB", (520, 1100), "white")
            draw = ImageDraw.Draw(image)
            plugin._draw_throughput_panel(draw, (0, 0, 520, 540), traffic, NOW, pytz.UTC, fonts, "12h", 100)
            plugin._draw_limit_panel(draw, (0, 550, 520, 1100), traffic, fonts, 100)
            assert image.getbbox() is not None


class TestLimitStats:
    def test_minutes_above_limit_tiers_and_peaks(self):
        import pytz
        from datetime import datetime
        from plugins.network_monitor.network_monitor import summarize_limit, summarize_throughput
        now = int(datetime(2026, 11, 11, 15, tzinfo=pytz.UTC).timestamp())
        mbit = lambda m: m * 1e6 / 8 * 60  # bytes in a minute at m Mbit/s
        down = [(now - 3600 + 60 * i, mbit(m), 60) for i, m in enumerate((120, 90, 60, 10))]
        up = [(now - 86400 - 1800, mbit(9), 60)]  # yesterday
        stats = summarize_limit({"down": down, "up": up}, 3, pytz.UTC, now, 100)
        assert stats["down_min"] == [None, None, 1.0] and stats["up_min"] == [None, 0.0, None]
        assert stats["tiers"] == {50.0: 3.0, 80.0: 2.0}
        assert round(stats["peak_down"]) == 120 and round(stats["peak_up"]) == 9 and stats["data_days"] > 1
        series = summarize_throughput({"down": down, "up": up}, now - 7200)
        assert [round(v) for _, v in series["down"]] == [120, 90, 60, 10] and series["up"] == []
