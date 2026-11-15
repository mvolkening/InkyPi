#!/usr/bin/env python3
"""Appliance disaggregation (NILM) from the whole-house power meter.

Splits the house's total power into individual appliances without a meter on each
one. Every switch-on or switch-off shows up as a step in the meter's per-leg real
and reactive power. The step's shape identifies the appliance:

  * legs: a 240 V appliance (water heater, range, well pump) steps both legs by the
    same amount; a 120 V one steps only leg A or leg B;
  * size: real power in W;
  * reactive power: motors and compressors (pumps, fridges, furnace blower) draw
    reactive power, heating elements (kettle, toaster, water heater) do not.

Each ON step is paired with the matching OFF step to make a cycle (start, duration,
energy). Cycles with the same signature are grouped under a stable load id
("L002"), so an appliance gets an id the first time it is seen and keeps it. Name
it whenever you know what it is (`--label L002 "Hot water tank"`, or the [labels]
section of the config); history is kept per id, so a name applies retroactively.

Plain-language hints help with naming: a guess from the signature ("240 V heating
element, ~6 min cycles: water heater?") and, for appliances Home Assistant knows
about (switched lights, metered plugs, the fridge door), how often an HA switch
change lines up with the load's steps.

Input: the PowerMeter bucket Node-RED fills in InfluxDB (one reading every ~2 s
per message type), read with a read-only token. Output, stdlib only, no pip installs:

  * SQLite file next to the state (/var/lib/nilm/nilm.db):
      event(ts, dir on|off, kind A|B|AB, da, db, dp, dq, settle, forced, load)   every step
      cycle(start, load, kind, p_w, q_var, duration_s, energy_wh, suspect)       every ON..OFF
  * Home Assistant via MQTT discovery: one device per named or recurring load, with
    a power (W) and an energy (kWh, total_increasing) sensor, plus "identified",
    "always-on" and "other" power on a summary device. HA keeps their history.

On first start it works through the stored history (backfill_days), so the loads
are already learned when it goes live.
"""

import argparse
import configparser
import csv
import io
import json
import logging
import math
import os
import re
import signal
import socket
import sqlite3
import struct
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

logger = logging.getLogger("nilm")

DEFAULT_CONFIG_PATH = "/etc/nilm/nilm.conf"
DEFAULT_STATE_PATH = "/var/lib/nilm/state.json"
STATE_VERSION = 1

DEFAULTS = {
    "influx": {
        "url": "http://localhost:8086",
        "org": "none",
        "token": "",
        "source_bucket": "PowerMeter",
        "ha_bucket": "homeassistant",
    },
    "mqtt": {
        "enabled": "true",
        "host": "localhost",
        "port": "1883",
        "username": "",
        "password": "",
        "discovery_prefix": "homeassistant",
    },
    "nilm": {
        "timezone": "America/Toronto",
        "poll_seconds": "60",
        # Reading lag: Node-RED writes each sample as it arrives, so only skip the last few seconds.
        "lag_seconds": "10",
        "backfill_days": "120",
        # A load is published (and listed) once it has this many cycles on this many days.
        "min_cycles": "10",
        "min_days": "2",
        "publish_max": "40",
    },
    "labels": {},
}


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

class Config:
    """Config file that reloads when its mtime changes (labels can be edited live)."""

    def __init__(self, path):
        self.path = path
        self._mtime = None
        self._parser = self._new_parser()
        self.reload_if_changed()

    @staticmethod
    def _new_parser():
        parser = configparser.ConfigParser(interpolation=None)
        parser.optionxform = str  # keep label ids as written
        parser.read_dict(DEFAULTS)
        return parser

    def reload_if_changed(self):
        try:
            mtime = os.stat(self.path).st_mtime
        except OSError:
            if self._mtime is None:
                logger.warning("Config file %s not found; using defaults.", self.path)
                self._mtime = 0
            return False
        if mtime == self._mtime:
            return False
        parser = self._new_parser()
        try:
            parser.read(self.path)
        except configparser.Error:
            logger.exception("Could not parse %s; keeping the previous settings.", self.path)
            return False
        first = self._mtime is None
        self._parser, self._mtime = parser, mtime
        if not first:
            logger.info("Reloaded configuration from %s.", self.path)
        return True

    def get(self, section, key):
        return self._parser.get(section, key)

    def get_int(self, section, key):
        try:
            return int(float(self.get(section, key)))
        except ValueError:
            return int(DEFAULTS[section][key])

    def get_bool(self, section, key):
        return self.get(section, key).strip().lower() in ("1", "true", "yes", "on")

    def labels(self):
        """{load id: name}; name "ignore" hides a load."""
        if not self._parser.has_section("labels"):
            return {}
        return {k.strip().upper(): v.strip() for k, v in self._parser.items("labels") if v.strip()}


def set_label(path, load_id, name):
    """Writes `load_id = name` into the [labels] section (removes it when name is
    empty), keeping the rest of the file and its comments as they are."""
    load_id = load_id.strip().upper()
    with open(path) as f:
        lines = f.read().splitlines()
    start = next((i for i, l in enumerate(lines) if l.strip().lower() == "[labels]"), None)
    if start is None:
        lines += ["", "[labels]"]
        start = len(lines) - 1
    end = next((i for i in range(start + 1, len(lines)) if lines[i].strip().startswith("[")), len(lines))
    pattern = re.compile(r"^\s*" + re.escape(load_id) + r"\s*[=:]", re.IGNORECASE)
    hit = next((i for i in range(start + 1, end) if pattern.match(lines[i])), None)
    if hit is not None:
        if name:
            lines[hit] = f"{load_id} = {name}"
        else:
            del lines[hit]
    elif name:
        insert = end
        while insert > start + 1 and not lines[insert - 1].strip():
            insert -= 1
        lines.insert(insert, f"{load_id} = {name}")
    # The file holds the InfluxDB token: keep its owner and permissions.
    st = os.stat(path)
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, st.st_mode & 0o777)
    with os.fdopen(fd, "w") as f:
        f.write("\n".join(lines) + "\n")
    if hasattr(os, "chown"):
        try:
            os.chown(tmp, st.st_uid, st.st_gid)
        except OSError:
            pass
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# InfluxDB (v2 HTTP API)
# ---------------------------------------------------------------------------

class InfluxError(RuntimeError):
    pass


def _flux_str(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _rfc3339(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


_minute_cache = {}


def _parse_time(value):
    """Epoch seconds from an Influx RFC 3339 timestamp (fast path for the many
    samples that share a minute)."""
    if not value or len(value) < 19:
        return None
    minute = value[:16]
    base = _minute_cache.get(minute)
    if base is None:
        try:
            base = int(datetime.strptime(minute, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            return None
        if len(_minute_cache) > 5000:
            _minute_cache.clear()
        _minute_cache[minute] = base
    try:
        return base + int(value[17:19])
    except ValueError:
        return None


class Influx:
    def __init__(self, config):
        self.config = config

    def _request(self, path, params, data, headers, timeout=60):
        url = self.config.get("influx", "url").rstrip("/") + path + "?" + urllib.parse.urlencode(params)
        headers = dict(headers, Authorization="Token " + self.config.get("influx", "token"))
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raise InfluxError(f"HTTP {e.code} from {path}: {e.read().decode('utf-8', 'replace')[:200]}")
        except (urllib.error.URLError, OSError) as e:
            raise InfluxError(f"Could not reach InfluxDB: {e}")

    def bucket(self, key):
        return self.config.get("influx", key)

    def query(self, flux):
        text = self._request("/api/v2/query", {"org": self.config.get("influx", "org")}, flux.encode("utf-8"),
                             {"Content-Type": "application/vnd.flux", "Accept": "application/csv"}, timeout=180)
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


Sample = tuple  # (ts, pa, pb, qa, qb); unused fields are None


def _float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fetch_samples(influx, start_ts, stop_ts):
    """Meter readings in [start_ts, stop_ts), oldest first. The meter sends real
    power (S=1) and reactive power (S=2) as separate messages, so a row usually has
    only one of the two pairs."""
    rows = influx.query(
        f'from(bucket: "{_flux_str(influx.bucket("source_bucket"))}") '
        f'|> range(start: {_rfc3339(start_ts)}, stop: {_rfc3339(stop_ts)}) '
        '|> filter(fn: (r) => r._measurement == "PowerMeter" and (r._field == "PowerA" or r._field == "PowerB" '
        'or r._field == "PowerReactiveA" or r._field == "PowerReactiveB")) '
        '|> group() |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value") '
        '|> keep(columns: ["_time", "PowerA", "PowerB", "PowerReactiveA", "PowerReactiveB"])'
    )
    out = []
    for r in rows:
        ts = _parse_time(r.get("_time"))
        if ts is None:
            continue
        out.append((ts, _float(r.get("PowerA")), _float(r.get("PowerB")),
                    _float(r.get("PowerReactiveA")), _float(r.get("PowerReactiveB"))))
    out.sort(key=lambda s: s[0])
    return out


# ---------------------------------------------------------------------------
# MQTT (minimal 3.1.1 publisher, QoS 0)
# ---------------------------------------------------------------------------

def _mqtt_len(n):
    out = bytearray()
    while True:
        byte, n = n % 128, n // 128
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _mqtt_str(s):
    b = s.encode("utf-8")
    return struct.pack("!H", len(b)) + b


class MqttPublisher:
    """Connects, publishes a batch, disconnects; enough for a once-a-minute update."""

    def __init__(self, host, port, username="", password="", client_id="nilm"):
        self.host, self.port = host, port
        self.username, self.password = username, password
        self.client_id = client_id

    def publish_many(self, messages):
        """messages: [(topic, payload str, retain bool)]."""
        flags = 0x02
        payload = _mqtt_str(self.client_id)
        if self.username:
            flags |= 0x80
            payload += _mqtt_str(self.username)
            if self.password:
                flags |= 0x40
                payload += _mqtt_str(self.password)
        variable = _mqtt_str("MQTT") + bytes([4, flags]) + struct.pack("!H", 60)
        with socket.create_connection((self.host, self.port), timeout=10) as sock:
            body = variable + payload
            sock.sendall(b"\x10" + _mqtt_len(len(body)) + body)
            ack = sock.recv(4)
            if len(ack) < 4 or ack[0] != 0x20 or ack[3] != 0:
                raise OSError(f"MQTT connect refused: {ack!r}")
            for topic, text, retain in messages:
                body = _mqtt_str(topic) + text.encode("utf-8")
                sock.sendall(bytes([0x30 | (1 if retain else 0)]) + _mqtt_len(len(body)) + body)
            sock.sendall(b"\xe0\x00")


# ---------------------------------------------------------------------------
# results (SQLite)
# ---------------------------------------------------------------------------

class ResultStore:
    """Every detected step and cycle, so loads can be re-examined (or re-learned) later."""

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS event (
            ts INTEGER, dir TEXT, kind TEXT, da REAL, db REAL, dp REAL, dq REAL,
            settle INTEGER, forced INTEGER, load TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (ts, dir, kind));
        CREATE TABLE IF NOT EXISTS cycle (
            start INTEGER, load TEXT, kind TEXT, p_w REAL, q_var REAL, duration_s INTEGER,
            energy_wh REAL, suspect INTEGER, PRIMARY KEY (start, load));
        CREATE INDEX IF NOT EXISTS cycle_by_load ON cycle (load, start);
    """

    def __init__(self, path):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.executescript(self.SCHEMA)

    def apply(self, records):
        """Records from Engine.process: ("event", ts, dir, kind, da, db, dp, dq, settle, forced),
        ("load", ts, dir, kind, load id) and ("cycle", start, load, kind, p, q, duration, wh, suspect)."""
        with self.db:
            for rec in records:
                if rec[0] == "event":
                    self.db.execute("INSERT OR REPLACE INTO event VALUES (?,?,?,?,?,?,?,?,?,'')", rec[1:])
                elif rec[0] == "load":
                    self.db.execute("UPDATE event SET load=? WHERE ts=? AND dir=? AND kind=?",
                                    (rec[4], rec[1], rec[2], rec[3]))
                else:
                    self.db.execute("INSERT OR REPLACE INTO cycle VALUES (?,?,?,?,?,?,?,?)", rec[1:])

    def delete_all(self):
        with self.db:
            self.db.execute("DELETE FROM event")
            self.db.execute("DELETE FROM cycle")

    def events_since(self, since_ts):
        """[(ts, +1 on / -1 off, load id or '')], oldest first."""
        rows = self.db.execute("SELECT ts, dir, load FROM event WHERE ts >= ? ORDER BY ts", (since_ts,))
        return [(ts, 1 if d == "on" else -1, load) for ts, d, load in rows]

    def close(self):
        self.db.close()


# ---------------------------------------------------------------------------
# step detection
# ---------------------------------------------------------------------------

ABS_TOL_W = 10.0          # a reading within this (or REL_TOL of the level) belongs to the current level
REL_TOL = 0.02
STABLE_SAMPLES = 3        # a new level must hold for this many readings (~6 s)
MAX_SPIKE_SAMPLES = 6     # an excursion that returns within this many readings is an inrush spike
MAX_PENDING_SAMPLES = 30  # a level that never settles is accepted after ~60 s anyway
MIN_STEP_W = 30.0
SEG_MAX_N = 20            # the level tracks slow drift with this much memory
BALANCED_RATIO = 0.35     # both legs within this ratio of each other = a 240 V load
DATA_GAP_SECONDS = 120


def _tol(level):
    return max(ABS_TOL_W, REL_TOL * abs(level))


def leg_kind(da, db):
    """'AB' for a 240 V load (both legs step together), else the leg that moved."""
    big, small = max(abs(da), abs(db)), min(abs(da), abs(db))
    if da * db > 0 and small >= BALANCED_RATIO * big:
        return "AB"
    return "A" if abs(da) >= abs(db) else "B"


class Event:
    FIELDS = ("ts", "da", "db", "dqa", "dqb", "base_a", "base_b", "settle", "forced", "q_before", "load_id")
    __slots__ = FIELDS + ("kind",)

    def __init__(self, ts, da, db, base_a=0.0, base_b=0.0, settle=0, forced=False, q_before=None):
        self.ts, self.da, self.db = ts, da, db
        self.base_a, self.base_b = base_a, base_b
        self.settle, self.forced = settle, forced
        self.q_before = q_before
        self.dqa = self.dqb = 0.0
        self.load_id = None
        self.kind = leg_kind(da, db)

    @property
    def dp(self):
        return self.da + self.db

    @property
    def dq(self):
        return self.dqa + self.dqb

    @property
    def sign(self):
        return 1 if self.dp > 0 else -1

    def to_dict(self):
        return {k: getattr(self, k) for k in self.FIELDS}

    @classmethod
    def from_dict(cls, d):
        e = cls(d["ts"], d["da"], d["db"])
        for k in cls.FIELDS:
            setattr(e, k, d.get(k))
        e.dqa, e.dqb = e.dqa or 0.0, e.dqb or 0.0
        return e


class EdgeDetector:
    """Steady-state step detector over both legs' real power: a step is a change
    from one stable level to another; inrush spikes that settle back are ignored.
    Each step also gets the change in reactive power, from the readings just
    before and just after it."""

    def __init__(self):
        self.ma = self.mb = None
        self.n = 0
        self.pending = []
        self.qa = self.qb = None
        self.q_seg = [0.0, 0.0, 0]
        self.awaiting_q = []
        self.ready = []
        self.last_ts = None

    def to_dict(self):
        return {"ma": self.ma, "mb": self.mb, "n": self.n, "pending": self.pending, "qa": self.qa, "qb": self.qb,
                "q_seg": self.q_seg, "awaiting_q": [e.to_dict() for e in self.awaiting_q], "last_ts": self.last_ts}

    @classmethod
    def from_dict(cls, d):
        det = cls()
        det.ma, det.mb, det.n = d.get("ma"), d.get("mb"), d.get("n", 0)
        det.pending = [tuple(s) for s in d.get("pending", [])]
        det.qa, det.qb = d.get("qa"), d.get("qb")
        det.q_seg = list(d.get("q_seg", [0.0, 0.0, 0]))
        det.awaiting_q = [Event.from_dict(e) for e in d.get("awaiting_q", [])]
        det.last_ts = d.get("last_ts")
        return det

    @property
    def level(self):
        return None if self.ma is None else self.ma + self.mb

    def _start_segment(self, samples):
        self._finish_q()
        self.ma = sum(s[1] for s in samples) / len(samples)
        self.mb = sum(s[2] for s in samples) / len(samples)
        self.n = len(samples)
        self.q_seg = [0.0, 0.0, 0]

    @staticmethod
    def _within(s, ma, mb):
        return abs(s[1] - ma) <= _tol(ma) and abs(s[2] - mb) <= _tol(mb)

    def _add_q(self, s):
        if s[3] is None or s[4] is None:
            return
        self.qa, self.qb = s[3], s[4]
        qs = self.q_seg
        qs[0] += s[3]
        qs[1] += s[4]
        qs[2] += 1
        if qs[2] >= 2:
            self._finish_q()

    def _finish_q(self):
        qs = self.q_seg
        for e in self.awaiting_q:
            if qs[2] and e.q_before is not None:
                e.dqa, e.dqb = qs[0] / qs[2] - e.q_before[0], qs[1] / qs[2] - e.q_before[1]
            self.ready.append(e)
        self.awaiting_q = []

    def _q_mean(self):
        qs = self.q_seg
        if qs[2]:
            return [qs[0] / qs[2], qs[1] / qs[2]]
        if self.qa is not None:
            return [self.qa, self.qb]
        return None

    def feed(self, s):
        """Feeds one reading (ts, pa, pb, qa, qb); returns the steps it completed."""
        self._step(s)
        if not self.ready:
            return ()
        out, self.ready = self.ready, []
        return out

    def _step(self, s):
        if s[1] is None or s[2] is None:
            self._add_q(s)
            return
        if self.last_ts is not None and s[0] - self.last_ts > DATA_GAP_SECONDS:
            # What happened during a data gap is unknown; start from the new level.
            self._finish_q()
            self.pending = []
            self.ma = None
        self.last_ts = s[0]
        if self.ma is None:
            self._start_segment([s])
            self._add_q(s)
            return

        if not self.pending:
            if self._within(s, self.ma, self.mb):
                k = min(self.n, SEG_MAX_N)
                self.ma = (self.ma * k + s[1]) / (k + 1)
                self.mb = (self.mb * k + s[2]) / (k + 1)
                self.n += 1
                self._add_q(s)
            else:
                self.pending = [s]
            return

        if self._within(s, self.ma, self.mb) and len(self.pending) <= MAX_SPIKE_SAMPLES:
            self.pending = []
            self._add_q(s)
            return

        self.pending.append(s)
        if len(self.pending) < STABLE_SAMPLES:
            return
        tail = self.pending[-STABLE_SAMPLES:]
        ta = sum(x[1] for x in tail) / STABLE_SAMPLES
        tb = sum(x[2] for x in tail) / STABLE_SAMPLES
        stable = all(self._within(x, ta, tb) for x in tail)
        forced = not stable and len(self.pending) >= MAX_PENDING_SAMPLES
        if not (stable or forced):
            return
        da, db = ta - self.ma, tb - self.mb
        event = None
        if abs(da + db) >= MIN_STEP_W:
            event = Event(self.pending[0][0], da, db, self.ma, self.mb,
                          tail[0][0] - self.pending[0][0], forced, self._q_mean())
        self._start_segment(tail)
        self.pending = []
        if event is not None:
            self.awaiting_q.append(event)
        for x in tail:
            self._add_q(x)


# ---------------------------------------------------------------------------
# pairing steps into cycles, grouping cycles into loads
# ---------------------------------------------------------------------------

PAIR_ABS_TOL_W = 30.0
PAIR_REL_TOL = 0.12
MAX_ON_SECONDS = 12 * 3600
SINGLE_INSTANCE_W = 100.0  # loads at least this big can't be switched on twice at once
RECENT_STARTS_S = 48 * 3600  # start times kept per load, for "runs today / overnight"
DUR_BINS = 24              # log2 duration histogram: bin k holds durations in [2^k, 2^(k+1)) s


def _pair_tol(p):
    return max(PAIR_ABS_TOL_W, PAIR_REL_TOL * abs(p))


def _q_tol(on):
    return max(40.0, 0.3 * abs(on.dq), 0.15 * abs(on.dp))


class Cycle:
    __slots__ = ("on", "off", "kind", "p", "q", "load_id", "suspect")

    def __init__(self, on, off):
        self.on, self.off = on, off
        self.kind = on.kind
        self.p = (on.dp - off.dp) / 2.0
        self.q = (on.dq - off.dq) / 2.0
        self.load_id = None
        self.suspect = False

    @property
    def start(self):
        return self.on.ts

    @property
    def duration(self):
        return max(0, self.off.ts - self.on.ts)

    @property
    def energy_wh(self):
        return self.p * self.duration / 3600.0


class Pairer:
    """Matches each OFF step with the open ON step of the same legs and size."""

    def __init__(self, open_events=None):
        self.open = open_events or []

    def feed(self, e):
        """Returns ([cycles], [ON steps dropped unmatched]). Usually zero or one cycle;
        an OFF step on both legs that matches nothing is tried as two simultaneous
        120 V switch-offs (one per leg), which can close two cycles."""
        cycle, dropped = self._feed(e)
        if cycle is not None or e.dp > 0 or min(abs(e.da), abs(e.db)) < MIN_STEP_W:
            return ([cycle] if cycle else []), dropped
        cycles = []
        for part in (Event(e.ts, e.da, 0.0), Event(e.ts, 0.0, e.db)):
            part.dqa, part.dqb = (e.dqa, 0.0) if part.kind == "A" else (0.0, e.dqb)
            match, _ = self._match(part)
            if match is not None:
                self.open.remove(match)
                cycles.append(Cycle(match, part))
        return cycles, dropped

    def _feed(self, e):
        dropped = [o for o in self.open if e.ts - o.ts > MAX_ON_SECONDS]
        if dropped:
            self.open = [o for o in self.open if e.ts - o.ts <= MAX_ON_SECONDS]
        if e.dp > 0:
            if e.dp >= SINGLE_INSTANCE_W:
                # An open ON of the same size means its OFF step was missed (hidden
                # inside another change); this new ON replaces it.
                dup = [o for o in self.open if o.kind == e.kind and abs(o.dp - e.dp) <= _pair_tol(o.dp)
                       and abs(o.dq - e.dq) <= _q_tol(o)]
                if dup:
                    self.open = [o for o in self.open if o not in dup]
                    dropped.extend(dup)
            self.open.append(e)
            return None, dropped
        best, _ = self._match(e)
        if best is None:
            return None, dropped
        self.open.remove(best)
        return Cycle(best, e), dropped

    def _match(self, e):
        """The open ON step that best matches OFF step `e`, or (None, None)."""
        best, best_score = None, None
        for o in self.open:
            if o.kind != e.kind:
                continue
            miss = abs(o.dp + e.dp) / _pair_tol(o.dp)
            if miss > 1.0:
                continue
            qmiss = abs(o.dq + e.dq) / _q_tol(o)
            score = miss + 0.5 * min(qmiss, 2.0) + (e.ts - o.ts) / MAX_ON_SECONDS
            if best_score is None or score < best_score:
                best, best_score = o, score
        return best, best_score


def load_distance(kind_a, p_a, q_a, kind_b, p_b, q_b, q_weight=1.0):
    if kind_a != kind_b:
        return math.inf
    p = max(abs(p_a), abs(p_b))
    dp = abs(p_a - p_b) / (0.08 * p + 15.0)
    dq = abs(q_a - q_b) / (0.15 * max(abs(q_a), abs(q_b)) + 0.05 * p + 20.0)
    return max(dp, dq * q_weight)


def _hist_quantile(hist, frac):
    """Approximate quantile (seconds, geometric bin centre) of a log2 histogram."""
    total = sum(hist)
    if not total:
        return None
    acc = 0
    for k, c in enumerate(hist):
        acc += c
        if acc >= frac * total:
            return 2 ** (k + 0.5)
    return 2 ** (len(hist) - 0.5)


class LoadBook:
    """Loads, each a recurring (legs, real power, reactive power) signature."""

    def __init__(self, loads=None, next_id=1):
        self.loads = loads or {}
        self.next_id = next_id

    def nearest(self, kind, p, q, q_weight=1.0):
        best, best_d = None, None
        for load in self.loads.values():
            d = load_distance(kind, p, q, load["kind"], load["p"], load["q"], q_weight)
            if d <= 1.0 and (best_d is None or d < best_d):
                best, best_d = load, d
        return best

    def new_load(self, kind, p, q, ts):
        load_id = "L%03d" % self.next_id
        self.next_id += 1
        load = self.loads[load_id] = {
            "id": load_id, "kind": kind, "p": p, "q": q, "n": 0, "first": ts, "last": ts,
            "days": 0, "last_day": None, "wh": 0.0, "dur_hist": [0] * DUR_BINS, "hours": [0] * 24, "daily": {},
        }
        return load

    def assign(self, cycle, local_dt):
        """Gives the cycle its load (creating one for a new signature) and updates it.
        A cycle far longer than the load usually runs is kept but flagged suspect
        (most likely its OFF step was missed and a later one matched): it doesn't
        count towards energy."""
        load = self.nearest(cycle.kind, cycle.p, cycle.q) or self.new_load(cycle.kind, cycle.p, cycle.q, cycle.start)
        hist = load["dur_hist"]
        typical = _hist_quantile(hist, 0.9)
        if load["n"] >= 20 and typical and cycle.duration > max(3600, 4 * typical):
            cycle.suspect = True
        w = min(load["n"], 50)
        load["p"] = (load["p"] * w + cycle.p) / (w + 1)
        load["q"] = (load["q"] * w + cycle.q) / (w + 1)
        load["n"] += 1
        load["last"] = max(load["last"], cycle.start)
        day = local_dt.strftime("%Y-%m-%d")
        if load["last_day"] != day:
            load["days"] += 1
            load["last_day"] = day
        hist[min(DUR_BINS - 1, int(math.log2(max(1, cycle.duration))))] += 1
        load["hours"][local_dt.hour] += 1
        recent = [t for t in load.get("recent", []) if t > cycle.start - RECENT_STARTS_S]
        recent.append(cycle.start)
        load["recent"] = recent
        daily = load["daily"].setdefault(day, [0, 0.0])
        daily[0] += 1
        if not cycle.suspect:
            load["wh"] += cycle.energy_wh
            daily[1] += cycle.energy_wh
        cycle.load_id = load["id"]
        return load

    def prune(self, now_ts, keep_days=30):
        """Forgets one-off signatures (fewer than 3 cycles, not seen for a month) and old daily totals."""
        cutoff_day = datetime.fromtimestamp(now_ts - keep_days * 86400, timezone.utc).strftime("%Y-%m-%d")
        for load_id in list(self.loads):
            load = self.loads[load_id]
            load["daily"] = {d: v for d, v in load["daily"].items() if d >= cutoff_day}
            if load["n"] < 3 and now_ts - load["last"] > keep_days * 86400:
                del self.loads[load_id]


# ---------------------------------------------------------------------------
# describing a load
# ---------------------------------------------------------------------------

def fmt_power(w):
    return f"{w / 1000:.2f} kW" if abs(w) >= 1000 else f"{w:.0f} W"


def fmt_duration(s):
    if s is None:
        return "?"
    if s < 90:
        return f"{s:.0f} s"
    if s < 5400:
        return f"{s / 60:.0f} min"
    return f"{s / 3600:.1f} h"


def describe_kind(load):
    return {"AB": "240 V", "A": "120 V leg A", "B": "120 V leg B"}[load["kind"]]


def reactive_class(load):
    p, q = abs(load["p"]), load["q"]
    if q > 0.4 * p:
        return "motor"
    if q > 0.15 * p:
        return "partly inductive"
    if q < -0.25 * p:
        return "electronic"
    return "resistive"


def recent_stats(load, today, days=14):
    """(cycles per day, kWh per day, kWh today) over the last `days` local days."""
    keys = sorted(load["daily"])[-days:]
    span = max(1, min(days, len(keys)))
    if keys:
        first = datetime.strptime(keys[0], "%Y-%m-%d")
        last = datetime.strptime(today, "%Y-%m-%d")
        span = max(1, min(days, (last - first).days + 1))
    cycles = sum(load["daily"][k][0] for k in keys)
    wh = sum(load["daily"][k][1] for k in keys)
    return cycles / span, wh / 1000 / span, load["daily"].get(today, [0, 0.0])[1] / 1000


def guess(load, cycles_per_day):
    """A plain-language guess from the signature alone; just a starting point for naming."""
    p, kind, rc = abs(load["p"]), load["kind"], reactive_class(load)
    med = _hist_quantile(load["dur_hist"], 0.5) or 0
    hours = load["hours"]
    total = sum(hours) or 1
    meals = sum(hours[h] for h in (6, 7, 8, 9, 11, 12, 13, 16, 17, 18, 19)) / total
    night = sum(hours[h] for h in (0, 1, 2, 3, 4)) / total
    if kind == "AB":
        if rc == "motor" or rc == "partly inductive":
            return "240 V motor: well pump, pool pump or furnace?"
        if p >= 2000 and med >= 120 and night > 0.03:
            return "240 V heating element running day and night: water heater?"
        if p >= 3500:
            return "240 V heating, large: oven, dryer or furnace element?"
        if p >= 600 and med < 120:
            return "240 V element cycling on/off: cooktop element on low/medium?"
        if p >= 600:
            return "240 V heating element: range/oven or baseboard heater?"
        return "small 240 V load"
    if rc == "motor":
        if p >= 300 and med <= 180:
            return "motor running briefly and often: water pump (pressure or sump)?"
        if p >= 300:
            return "motor: furnace blower, pool pump or dehumidifier?"
        if p >= 60 and 240 <= med <= 7200:
            return "compressor-like cycling: fridge, freezer or dehumidifier?"
        return "small motor or fan"
    if rc == "partly inductive" and 60 <= p <= 350 and 240 <= med <= 7200:
        return "compressor-like cycling: fridge or freezer?"
    if p >= 700 and med < 60 and meals > 0.5:
        return "kitchen heater cycling: coffee maker warming plate or cooktop?"
    if 900 <= p <= 1900 and med <= 1800:
        return "kitchen heating appliance: kettle, toaster, air fryer or microwave?"
    if p >= 900:
        return "large 120 V heater: space heater or air fryer?"
    if rc == "electronic":
        return "electronics (TV, computer, LED driver)?"
    if p < 300:
        return "lights or small electronics?"
    return None


# ---------------------------------------------------------------------------
# Home Assistant correlation hints
# ---------------------------------------------------------------------------

HA_MATCH_WINDOW_S = 45
HA_OFF_STATES = {"off", "power_off", "standby", "idle", "initial", "unknown", "unavailable", "0", "0.0", "false",
                 "closed", "none", ""}


def ha_transitions(influx, since_ts):
    """{entity name: [(ts, +1 on / -1 off)]} for HA switches, lights, door sensors,
    status enums and plug power readings (on = above 10 W) in the HA bucket."""
    rows = influx.query(
        f'from(bucket: "{_flux_str(influx.bucket("ha_bucket"))}") |> range(start: {_rfc3339(since_ts)}) '
        '|> filter(fn: (r) => r._measurement == "switch" or r._measurement == "light" '
        'or r._measurement == "binary_sensor__door" or r._measurement == "sensor__power" or r._measurement == "sensor__enum") '
        '|> filter(fn: (r) => r._field == "value" or r._field == "state") '
        # HA writes numbers and text states as different field types; one type for all.
        '|> toString() |> keep(columns: ["_time", "_value", "_measurement", "entity_id"])'
    )
    series = {}
    for r in rows:
        ts = _parse_time(r.get("_time"))
        if ts is None:
            continue
        measurement, value = r.get("_measurement", ""), r.get("_value", "")
        if measurement == "sensor__power":
            v = _float(value)
            if v is None:
                continue
            state = v > 10.0
        else:
            state = str(value).strip().lower() not in HA_OFF_STATES
        name = measurement.split("__")[0] + "." + r.get("entity_id", "")
        series.setdefault(name, []).append((ts, state))
    out = {}
    for name, points in series.items():
        points.sort()
        trans, prev = [], None
        for ts, state in points:
            if prev is not None and state != prev:
                trans.append((ts, 1 if state else -1))
            prev = state
        if trans:
            out[name] = trans
    return out


def ha_matches(transitions, events):
    """For each load, the HA entity whose on/off changes most often coincide with the
    load's own steps: {load id: (entity, matched changes, entity changes, load steps)}."""
    import bisect
    times = [e[0] for e in events]
    load_steps = {}
    for _, _, load in events:
        if load:
            load_steps[load] = load_steps.get(load, 0) + 1
    best = {}
    for entity, trans in transitions.items():
        counts = {}
        for ts, sign in trans:
            i = bisect.bisect_left(times, ts - HA_MATCH_WINDOW_S)
            pick = None
            while i < len(events) and events[i][0] <= ts + HA_MATCH_WINDOW_S:
                ev = events[i]
                if ev[1] == sign and ev[2] and (pick is None or abs(ev[0] - ts) < abs(pick[0] - ts)):
                    pick = ev
                i += 1
            if pick:
                counts[pick[2]] = counts.get(pick[2], 0) + 1
        for load, c in counts.items():
            # Agreement both ways: share of the entity's changes and of the load's steps.
            score = c / math.sqrt(len(trans) * max(1, load_steps.get(load, 1)))
            if c >= 3 and c >= 0.3 * len(trans) and (load not in best or score > best[load][4]):
                best[load] = (entity, c, len(trans), load_steps.get(load, 0), score)
    return {k: v[:4] for k, v in best.items()}


def describe_match(match):
    entity, c, n_entity, n_load = match
    return f"{entity} ({c} of its {n_entity} on/off changes line up with this load's steps)"


# ---------------------------------------------------------------------------
# engine: detector + pairer + loads, persisted between runs
# ---------------------------------------------------------------------------

BASE_WINDOW_S = 600       # always-on load = lowest 10-minute average over the last day
BASE_WINDOWS = 144


class Engine:
    def __init__(self, tz):
        self.tz = tz
        self.det = EdgeDetector()
        self.pairer = Pairer()
        self.book = LoadBook()
        self.last_ts = None
        self.latest_total = None
        self.base_windows = deque(maxlen=BASE_WINDOWS)  # [window start, sum, count]
        self.hints = {}       # load id -> ha match tuple
        self.published = {}   # group key -> [topics]
        self.last_hints = 0

    # -- persistence -----------------------------------------------------------

    def to_dict(self):
        return {
            "version": STATE_VERSION, "last_ts": self.last_ts, "detector": self.det.to_dict(),
            "open": [e.to_dict() for e in self.pairer.open], "loads": self.book.loads, "next_id": self.book.next_id,
            "latest_total": self.latest_total, "base_windows": list(self.base_windows), "hints": self.hints,
            "published": self.published, "last_hints": self.last_hints,
        }

    @classmethod
    def from_dict(cls, d, tz):
        eng = cls(tz)
        if d.get("version") != STATE_VERSION:
            return eng
        eng.last_ts = d.get("last_ts")
        eng.det = EdgeDetector.from_dict(d.get("detector", {}))
        eng.pairer = Pairer([Event.from_dict(e) for e in d.get("open", [])])
        eng.book = LoadBook(d.get("loads", {}), d.get("next_id", 1))
        eng.latest_total = d.get("latest_total")
        eng.base_windows = deque((list(w) for w in d.get("base_windows", [])), maxlen=BASE_WINDOWS)
        eng.hints = {k: tuple(v) for k, v in d.get("hints", {}).items()}
        eng.published = d.get("published", {})
        eng.last_hints = d.get("last_hints", 0)
        return eng

    def reset_keep_ids(self):
        """Fresh start (for a rebuild) that keeps each known load's id and signature,
        so names given to ids stay attached to the same appliances."""
        old = self.book
        self.__init__(self.tz)
        for load_id, load in old.loads.items():
            if load["n"] >= 3:
                seeded = self.book.new_load(load["kind"], load["p"], load["q"], load["first"])
                del self.book.loads[seeded["id"]]
                seeded["id"] = load_id
                self.book.loads[load_id] = seeded
        self.book.next_id = old.next_id

    # -- processing ------------------------------------------------------------

    def local(self, ts):
        return datetime.fromtimestamp(ts, self.tz)

    def process(self, samples):
        """Feeds readings; returns the records to store (see ResultStore.apply)."""
        records = []
        for s in samples:
            if self.last_ts is not None and s[0] <= self.last_ts:
                continue
            self.last_ts = s[0]
            if s[1] is not None and s[2] is not None:
                self._track_base(s[0], s[1] + s[2])
            for e in self.det.feed(s):
                records.append(("event", e.ts, "on" if e.sign > 0 else "off", e.kind, round(e.da, 1), round(e.db, 1),
                                round(e.dp, 1), round(e.dq, 1), int(e.settle or 0), int(bool(e.forced))))
                cycles, _dropped = self.pairer.feed(e)
                for cycle in cycles:
                    load = self.book.assign(cycle, self.local(cycle.start))
                    cycle.on.load_id = cycle.off.load_id = load["id"]
                    # The OFF step is stored as detected (a split one keeps its both-legs kind).
                    records.append(("load", cycle.on.ts, "on", cycle.on.kind, load["id"]))
                    records.append(("load", e.ts, "off", e.kind, load["id"]))
                    records.append(("cycle", cycle.start, load["id"], cycle.kind, round(cycle.p, 1), round(cycle.q, 1),
                                    cycle.duration, round(cycle.energy_wh, 2), int(cycle.suspect)))
        return records

    def _track_base(self, ts, total):
        self.latest_total = total
        start = ts - ts % BASE_WINDOW_S
        if self.base_windows and self.base_windows[-1][0] == start:
            w = self.base_windows[-1]
            w[1] += total
            w[2] += 1
        else:
            self.base_windows.append([start, total, 1])

    def base_load(self):
        means = [w[1] / w[2] for w in list(self.base_windows)[:-1] if w[2] >= 30]
        return min(means) if len(means) >= 6 else None

    def live_power(self):
        """{load id: W} for loads that are on right now (open ON steps that match a
        known load and haven't run much longer than that load usually does)."""
        out = {}
        now = self.last_ts or 0
        for e in self.pairer.open:
            load = self.book.nearest(e.kind, e.dp, e.dq) or self.book.nearest(e.kind, e.dp, e.dq, q_weight=0.0)
            if load is None or load["n"] < 3:
                continue
            typical = _hist_quantile(load["dur_hist"], 0.95) or 0
            if now - e.ts > max(1800, 3 * typical):
                continue
            out[load["id"]] = out.get(load["id"], 0.0) + load["p"]
        return out


# ---------------------------------------------------------------------------
# Home Assistant publishing
# ---------------------------------------------------------------------------

def _slug(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_") or "load"


def confirmed(load, cfg):
    return load["n"] >= cfg.get_int("nilm", "min_cycles") and load["days"] >= cfg.get_int("nilm", "min_days")


def publish_groups(engine, cfg):
    """{group key: {"name", "ids": [...]}}: named loads (one group per name, so ids
    given the same name are added up) plus the biggest recurring unnamed ones."""
    labels = cfg.labels()
    today = engine.local(engine.last_ts or time.time()).strftime("%Y-%m-%d")
    groups = {}
    unnamed = []
    for load in engine.book.loads.values():
        name = labels.get(load["id"])
        if name and name.lower() == "ignore":
            continue
        if name:
            groups.setdefault("nilm_" + _slug(name), {"name": name, "ids": []})["ids"].append(load["id"])
        elif confirmed(load, cfg):
            unnamed.append((recent_stats(load, today)[1], load))
    unnamed.sort(key=lambda x: -x[0])
    room = max(0, cfg.get_int("nilm", "publish_max") - len(groups))
    for _, load in unnamed[:room]:
        name = f"{load['id']} ({fmt_power(load['p'])}, {describe_kind(load)})"
        groups["nilm_" + load["id"].lower()] = {"name": name, "ids": [load["id"]]}
    return groups


def runs_summary(engine, loads):
    """Starts today, starts between midnight and 6 am (last night, or so far tonight),
    and the last start (ISO time) for a group of loads."""
    starts = sorted(t for l in loads for t in l.get("recent", []))
    now = engine.local(engine.last_ts or time.time())
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    return {"today": sum(1 for t in starts if t >= midnight),
            "overnight": sum(1 for t in starts if midnight <= t < midnight + 6 * 3600),
            "last": engine.local(starts[-1]).isoformat() if starts else None}


def build_messages(engine, cfg):
    prefix = cfg.get("mqtt", "discovery_prefix").strip() or "homeassistant"
    groups = publish_groups(engine, cfg)
    live = engine.live_power()
    today = engine.local(engine.last_ts or time.time()).strftime("%Y-%m-%d")
    messages, published = [], {}
    identified = 0.0

    def sensor(key, object_id, name, device, unit, device_class, state_class, value_template, attrs=False):
        topic = f"{prefix}/sensor/nilm/{object_id}/config"
        config = {
            "name": name, "unique_id": object_id, "default_entity_id": f"sensor.{object_id}",
            "state_topic": f"nilm/{key}/state", "value_template": value_template,
            "unit_of_measurement": unit, "device_class": device_class, "state_class": state_class,
            "expire_after": 900, "device": device,
        }
        config = {k: v for k, v in config.items() if v is not None}
        if attrs:
            config["json_attributes_topic"] = f"nilm/{key}/attributes"
        messages.append((topic, json.dumps(config), True))
        published.setdefault(key, []).append(topic)

    for key, group in sorted(groups.items()):
        loads = [engine.book.loads[i] for i in group["ids"] if i in engine.book.loads]
        if not loads:
            continue
        device = {"identifiers": [key], "name": group["name"], "manufacturer": "nilm (house meter)",
                  "model": "Detected load " + ", ".join(sorted(group["ids"]))}
        sensor(key, f"{key}_power", "Power", device, "W", "power", "measurement", "{{ value_json.power }}", attrs=True)
        sensor(key, f"{key}_energy", "Energy", device, "kWh", "energy", "total_increasing", "{{ value_json.energy }}")
        sensor(key, f"{key}_runs_today", "Runs today", device, "runs", None, "total_increasing", "{{ value_json.runs_today }}")
        sensor(key, f"{key}_last_run", "Last run", device, None, "timestamp", None, "{{ value_json.last_run }}")
        power = sum(live.get(l["id"], 0.0) for l in loads)
        identified += power
        stats = [recent_stats(l, today) for l in loads]
        main = max(loads, key=lambda l: l["wh"])
        attrs = {
            "load_ids": sorted(group["ids"]),
            "signature": "; ".join(f"{l['id']}: {fmt_power(l['p'])}, {describe_kind(l)}, {reactive_class(l)}" for l in loads),
            "typical_run": fmt_duration(_hist_quantile(main["dur_hist"], 0.5)),
            "cycles_per_day": round(sum(s[0] for s in stats), 1),
            "kwh_per_day": round(sum(s[1] for s in stats), 2),
            "kwh_today": round(sum(s[2] for s in stats), 3),
            "first_seen": engine.local(min(l["first"] for l in loads)).strftime("%Y-%m-%d"),
            "looks_like": guess(main, stats[0][0]) or "",
            "ha_match": "; ".join(describe_match(engine.hints[l["id"]]) for l in loads if l["id"] in engine.hints),
        }
        runs = runs_summary(engine, loads)
        attrs.update(runs_overnight=runs["overnight"])
        messages.append((f"nilm/{key}/state", json.dumps({"power": round(power, 1),
                                                          "energy": round(sum(l["wh"] for l in loads) / 1000, 3),
                                                          "runs_today": runs["today"], "last_run": runs["last"]}), False))
        messages.append((f"nilm/{key}/attributes", json.dumps(attrs), True))

    # Summary: what the named/recurring loads add up to, the always-on floor, and the rest.
    key = "nilm_summary"
    device = {"identifiers": [key], "name": "House loads", "manufacturer": "nilm (house meter)", "model": "Summary"}
    for field, name in (("identified", "Identified power"), ("base", "Always-on power"), ("other", "Other power")):
        sensor(key, f"nilm_{field}_power", name, device, "W", "power", "measurement", "{{ value_json.%s }}" % field)
    base = engine.base_load()
    total = engine.latest_total or 0.0
    state = {"identified": round(identified, 1), "base": round(base, 1) if base is not None else None,
             "other": round(max(0.0, total - identified - (base or 0.0)), 1)}
    messages.append((f"nilm/{key}/state", json.dumps(state), False))

    # Remove discovery entries of groups that went away (renamed, ignored, or no longer top-N).
    for old_key, topics in engine.published.items():
        for topic in topics:
            if topic not in published.get(old_key, []):
                messages.append((topic, "", True))
        if old_key not in published:
            messages.append((f"nilm/{old_key}/attributes", "", True))  # retained, so clear it too
    return messages, published


def publish(engine, cfg):
    if not cfg.get_bool("mqtt", "enabled"):
        return
    messages, published = build_messages(engine, cfg)
    MqttPublisher(cfg.get("mqtt", "host"), cfg.get_int("mqtt", "port"),
                  cfg.get("mqtt", "username"), cfg.get("mqtt", "password")).publish_many(messages)
    engine.published = published


# ---------------------------------------------------------------------------
# state file
# ---------------------------------------------------------------------------

def load_state(path, tz):
    try:
        with open(path) as f:
            return Engine.from_dict(json.load(f), tz)
    except (OSError, ValueError):
        return Engine(tz)


def save_state(path, engine):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(engine.to_dict(), f, separators=(",", ":"))
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# run modes
# ---------------------------------------------------------------------------

CHUNK_SECONDS = 6 * 3600
HINT_INTERVAL_S = 6 * 3600
HINT_DAYS = 14


def get_tz(cfg):
    name = cfg.get("nilm", "timezone").strip() or "UTC"
    try:
        return ZoneInfo(name)
    except Exception:
        logger.warning("Unknown timezone %r; using UTC.", name)
        return timezone.utc


def results_path(state_path):
    return os.path.join(os.path.dirname(state_path) or ".", "nilm.db")


def catch_up(engine, influx, results, cfg, state_path, stop_event=None, until=None):
    """Processes everything from the last reading seen up to `until` (default: now
    minus the reading lag), in chunks, saving progress after each one."""
    lag = cfg.get_int("nilm", "lag_seconds")
    until = until or int(time.time()) - lag
    if engine.last_ts is None:
        engine.last_ts = int(time.time()) - cfg.get_int("nilm", "backfill_days") * 86400
        logger.info("No state yet: learning from history since %s.", engine.local(engine.last_ts).strftime("%Y-%m-%d"))
    while engine.last_ts < until - 1 and not (stop_event and stop_event()):
        start = engine.last_ts + 1
        stop = min(until, start + CHUNK_SECONDS)
        samples = fetch_samples(influx, start, stop)
        records = engine.process(samples)
        if not samples or engine.last_ts < stop - 1:
            engine.last_ts = max(engine.last_ts, stop - 1)  # nothing (more) in this window
        results.apply(records)
        if until - stop > CHUNK_SECONDS:
            logger.info("Backfill at %s: %d loads so far.", engine.local(stop).strftime("%Y-%m-%d %H:%M"),
                        sum(1 for l in engine.book.loads.values() if l["n"] >= 10))
            save_state(state_path, engine)
    engine.book.prune(engine.last_ts)


def refresh_hints(engine, influx, results):
    since = int(time.time()) - HINT_DAYS * 86400
    engine.hints = ha_matches(ha_transitions(influx, since), results.events_since(since))
    engine.last_hints = time.time()


def run(cfg, state_path):
    influx = Influx(cfg)
    tz = get_tz(cfg)
    engine = load_state(state_path, tz)
    results = ResultStore(results_path(state_path))
    stopping = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stopping.append(True))
    logger.info("nilm started (config: %s, %d known loads).", cfg.path, len(engine.book.loads))
    while not stopping:
        loop_start = time.time()
        try:
            cfg.reload_if_changed()
            catch_up(engine, influx, results, cfg, state_path, stop_event=lambda: bool(stopping))
            now = time.time()
            if now - engine.last_hints > HINT_INTERVAL_S:
                try:
                    refresh_hints(engine, influx, results)
                except InfluxError as e:
                    logger.warning("Home Assistant hints skipped: %s", e)
                    engine.last_hints = now - HINT_INTERVAL_S + 600
            try:
                publish(engine, cfg)
            except OSError as e:
                logger.warning("MQTT publish failed: %s", e)
            save_state(state_path, engine)
        except InfluxError as e:
            logger.warning("InfluxDB problem (will retry): %s", e)
        except Exception:
            logger.exception("Unexpected error; continuing")
        delay = cfg.get_int("nilm", "poll_seconds") - (time.time() - loop_start)
        while delay > 0 and not stopping:
            time.sleep(min(1.0, delay))
            delay -= 1.0
    save_state(state_path, engine)
    results.close()
    return 0


def rebuild(cfg, state_path, since):
    """Re-learns everything from stored history (e.g. after changing the algorithm),
    keeping known load ids attached to their signatures."""
    influx = Influx(cfg)
    tz = get_tz(cfg)
    engine = load_state(state_path, tz)
    engine.reset_keep_ids()
    start = int(datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=tz).timestamp()) if since else \
        int(time.time()) - cfg.get_int("nilm", "backfill_days") * 86400
    logger.info("Deleting stored results and re-learning from %s.", engine.local(start).strftime("%Y-%m-%d"))
    results = ResultStore(results_path(state_path))
    results.delete_all()
    engine.last_ts = start
    catch_up(engine, influx, results, cfg, state_path)
    try:
        refresh_hints(engine, influx, results)
    except InfluxError as e:
        logger.warning("Home Assistant hints skipped: %s", e)
    save_state(state_path, engine)
    return 0


def report(cfg, state_path, show_all=False):
    tz = get_tz(cfg)
    engine = load_state(state_path, tz)
    if not engine.book.loads:
        print("No loads learned yet.")
        return 1
    try:
        refresh_hints(engine, Influx(cfg), ResultStore(results_path(state_path)))
    except (InfluxError, sqlite3.Error) as e:
        print(f"(Home Assistant hints unavailable: {e})")
    labels = cfg.labels()
    today = engine.local(engine.last_ts or time.time()).strftime("%Y-%m-%d")
    rows = []
    for load in engine.book.loads.values():
        if not show_all and not confirmed(load, cfg) and load["id"] not in labels:
            continue
        rows.append((recent_stats(load, today), load))
    rows.sort(key=lambda r: -r[0][1])
    total_kwh = sum(r[0][1] for r in rows)
    print(f"Loads learned up to {engine.local(engine.last_ts).strftime('%Y-%m-%d %H:%M')} "
          f"(last 14 days; {total_kwh:.1f} kWh/day identified; always-on floor "
          f"{fmt_power(engine.base_load() or 0)}).\n")
    print(f"{'id':5} {'name':22} {'legs':12} {'power':>8} {'reactive':>9}  {'type':16} {'runs':>7} {'/day':>6} {'kWh/d':>6}  hours (0-23)")
    for (per_day, kwh_day, _), load in rows:
        hours = load["hours"]
        peak = max(hours) or 1
        profile = "".join(" .:-=+*#"[min(7, round(7 * h / peak))] for h in hours)
        print(f"{load['id']:5} {labels.get(load['id'], '')[:22]:22} {describe_kind(load):12} {fmt_power(load['p']):>8} "
              f"{load['q']:>7.0f}var  {reactive_class(load):16} {fmt_duration(_hist_quantile(load['dur_hist'], 0.5)):>7} "
              f"{per_day:6.1f} {kwh_day:6.2f}  {profile}")
        hint = guess(load, per_day)
        if hint:
            print(f"{'':6}looks like: {hint}")
        if load["id"] in engine.hints:
            print(f"{'':6}HA match:   {describe_match(engine.hints[load['id']])}")
    print("\nName a load:  sudo nilm --label L002 \"Hot water tank\"   (same name on several ids adds them up;"
          " name \"ignore\" hides one)")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--config", default=os.environ.get("NILM_CONFIG", DEFAULT_CONFIG_PATH))
    parser.add_argument("--state", default=os.environ.get("NILM_STATE", DEFAULT_STATE_PATH))
    parser.add_argument("--report", action="store_true", help="list the learned loads and exit")
    parser.add_argument("--all", action="store_true", help="with --report: include loads seen only a few times")
    parser.add_argument("--label", nargs=2, metavar=("ID", "NAME"), help='name a load (NAME "" removes the name)')
    parser.add_argument("--rebuild", action="store_true", help="delete stored results and re-learn from history")
    parser.add_argument("--since", help="with --rebuild: start date YYYY-MM-DD (default: backfill_days ago)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(levelname)s %(message)s")
    if args.label:
        set_label(args.config, args.label[0], args.label[1].strip())
        print(f"{args.label[0].upper()} = {args.label[1].strip() or '(name removed)'}  (the service picks it up within a minute)")
        return 0
    cfg = Config(args.config)
    if args.report:
        return report(cfg, args.state, args.all)
    if args.rebuild:
        return rebuild(cfg, args.state, args.since)
    return run(cfg, args.state)


if __name__ == "__main__":
    sys.exit(main())
