import importlib.util
import json
import os
from zoneinfo import ZoneInfo

_PATH = os.path.join(os.path.dirname(__file__), "..", "scripts", "pidisplay_nilm", "nilm.py")
_spec = importlib.util.spec_from_file_location("nilm", _PATH)
nilm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(nilm)

T0 = 1_800_000_000
TZ = ZoneInfo("America/Toronto")


def _meter(loads, seconds, base=(150.0, 200.0)):
    """Readings every 2 s for `seconds`; loads = [(start, stop, da, db, dqa, dqb)]. Real and
    reactive power arrive as separate rows, like the meter's S=1 / S=2 messages."""
    rows = []
    for t in range(0, seconds, 2):
        pa, pb, qa, qb = base[0], base[1], -60.0, -40.0
        for start, stop, da, db, dqa, dqb in loads:
            if start <= t < stop:
                pa, pb, qa, qb = pa + da, pb + db, qa + dqa, qb + dqb
        rows.append((T0 + t, pa + (t % 3), pb, None, None))
        rows.append((T0 + t + 1, None, None, qa, qb))
    return rows


def _run(rows, engine=None):
    engine = engine or nilm.Engine(TZ)
    return engine, engine.process(rows)


def test_240v_heater_and_120v_pump_become_separate_loads():
    loads = []
    for k in range(4):  # water heater: 2.8 kW on both legs, 6 min every hour
        loads.append((k * 3600 + 600, k * 3600 + 960, 1400.0, 1400.0, 0.0, 0.0))
    for k in range(12):  # pump: 680 W leg B with matching reactive power, 24 s every 20 min
        loads.append((k * 1200 + 300, k * 1200 + 324, 0.0, 680.0, 0.0, 610.0))
    engine, records = _run(_meter(loads, 4 * 3600))
    cycles = [r for r in records if r[0] == "cycle"]
    by_load = {}
    for rec in cycles:
        by_load.setdefault(rec[2], []).append(rec)
    assert len(by_load) == 2
    heater = engine.book.loads[next(k for k, v in by_load.items() if v[0][3] == "AB")]
    pump = engine.book.loads[next(k for k, v in by_load.items() if v[0][3] == "B")]
    assert heater["n"] == 4 and abs(heater["p"] - 2800) < 60 and abs(heater["q"]) < 30
    assert pump["n"] == 12 and abs(pump["p"] - 680) < 30 and pump["q"] > 500
    assert nilm.reactive_class(pump) == "motor" and nilm.reactive_class(heater) == "resistive"
    assert "water pump" in nilm.guess(pump, 72)
    assert abs(sum(r[7] for r in by_load[heater["id"]]) - 4 * 2800 * 360 / 3600) < 150  # Wh


def test_simultaneous_switch_offs_on_both_legs_are_split():
    # Pump (leg A motor) and coffee plate (leg B) switch off within the same reading.
    pairer = nilm.Pairer()
    pump_on = nilm.Event(T0, 1100.0, 0.0)
    pump_on.dqa = 950.0
    plate_on = nilm.Event(T0 + 600, 0.0, 880.0)
    both_off = nilm.Event(T0 + 614, -1100.0, -870.0)
    both_off.dqa = -950.0
    assert both_off.kind == "AB"
    pairer.feed(pump_on)
    pairer.feed(plate_on)
    cycles, _ = pairer.feed(both_off)
    assert sorted((c.kind, round(c.p), c.duration) for c in cycles) == [("A", 1100, 614), ("B", 875, 14)]
    assert pairer.open == []


def test_inrush_spike_is_not_a_step():
    det = nilm.EdgeDetector()
    rows = [(T0 + 2 * i, 200.0, 300.0, None, None) for i in range(20)]
    rows[10] = (rows[10][0], 200.0, 1500.0, None, None)  # one-reading spike
    assert [e for r in rows for e in det.feed(r)] == []


def test_state_round_trip_continues_where_it_left_off():
    rows = _meter([(600, 960, 1400.0, 1400.0, 0.0, 0.0), (4200, 4560, 1400.0, 1400.0, 0.0, 0.0)], 2 * 3600)
    engine, first = _run(rows[: len(rows) // 2])
    restored = nilm.Engine.from_dict(json.loads(json.dumps(engine.to_dict())), TZ)
    _, second = _run(rows[len(rows) // 2:], restored)
    whole_engine, whole = _run(rows)
    assert [r for r in first + second if r[0] == "cycle"] == [r for r in whole if r[0] == "cycle"]


def test_results_store_and_ha_match(tmp_path):
    store = nilm.ResultStore(str(tmp_path / "nilm.db"))
    store.apply([("event", T0, "on", "B", 0.0, 120.0, 120.0, 0.0, 4, 0),
                 ("event", T0 + 600, "off", "B", 0.0, -118.0, -118.0, 0.0, 4, 0),
                 ("load", T0, "on", "B", "L007"), ("load", T0 + 600, "off", "B", "L007"),
                 ("cycle", T0, "L007", "B", 119.0, 0.0, 600, 19.8, 0)])
    events = store.events_since(0)
    assert events == [(T0, 1, "L007"), (T0 + 600, -1, "L007")]
    transitions = {"switch.living_room_light": [(T0 - 5, 1), (T0 + 610, -1), (T0 + 9000, 1)]}
    assert nilm.ha_matches(transitions, events * 2) == {}  # needs at least 3 matches
    many = [(T0 + k * 1000 + d, s, "L007") for k in range(4) for d, s in ((0, 1), (500, -1))]
    trans = {"switch.living_room_light": [(T0 + k * 1000 + d + 3, s) for k in range(4) for d, s in ((0, 1), (500, -1))]}
    assert nilm.ha_matches(trans, many)["L007"][:3] == ("switch.living_room_light", 8, 8)


def test_set_label_keeps_other_lines(tmp_path):
    conf = tmp_path / "nilm.conf"
    conf.write_text("[influx]\ntoken = x\n\n[labels]\n# comment\nL001 = Old\n")
    nilm.set_label(str(conf), "l002", "Hot water tank")
    nilm.set_label(str(conf), "L001", "")
    text = conf.read_text()
    assert "token = x" in text and "# comment" in text and "L002 = Hot water tank" in text and "L001" not in text
