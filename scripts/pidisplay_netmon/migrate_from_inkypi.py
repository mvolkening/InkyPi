#!/usr/bin/env python3
"""One-time import of the old InkyPi-hosted ping history into netmon's InfluxDB.

The previous Internet Outage Monitor plugin kept its data in a local SQLite file
(<InkyPi>/src/plugins/network_monitor/data/pings.db). Copy that file to the Pi
running netmon and run:

    python3 migrate_from_inkypi.py /path/to/pings.db

Uses the same /etc/netmon/netmon.conf as the daemon (so run it as a user that
can read it, e.g. with sudo). Safe to re-run: InfluxDB overwrites points that
share a series and timestamp, so nothing is duplicated.

Best run once before the daemon has been running long, but it also works
afterwards - the daemon and this script write to the same series, and imported
history simply fills in the earlier timeline.
"""

import argparse
import logging
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import netmon  # noqa: E402  (shares the config/InfluxDB client with the daemon)

BATCH = 5000

logger = logging.getLogger("migrate")


def write_batches(influx, bucket_key, lines):
    for i in range(0, len(lines), BATCH):
        influx.write(bucket_key, lines[i:i + BATCH])


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("db", help="path to the copied pings.db")
    parser.add_argument("--config", default=netmon.DEFAULT_CONFIG_PATH)
    parser.add_argument("--dry-run", action="store_true", help="count what would be imported, write nothing")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    influx = netmon.Influx(netmon.Config(args.config))

    # Failed pings (rtt NULL) become up=0 samples so outages still shade the sparklines.
    ping_lines = [
        f"ping up=1i,rtt_ms={rtt:.3f} {ts}" if rtt is not None else f"ping up=0i {ts}"
        for ts, rtt in conn.execute("SELECT ts, rtt_ms FROM pings ORDER BY ts")
    ]
    # The old schema didn't record whether an outage was local or ISP.
    outage_lines = [
        f'outage end_ts={end}i,scope="unknown" {start}'
        for start, end in conn.execute("SELECT start_ts, end_ts FROM outages WHERE end_ts IS NOT NULL")
    ]
    gap_lines = [f"gap end_ts={end}i {start}" for start, end in conn.execute("SELECT start_ts, end_ts FROM gaps")]
    meta_lines = [
        f"meta first_started_ts={int(value)}i {int(value)}"
        for (value,) in conn.execute("SELECT value FROM meta WHERE key = 'first_started_ts'")
    ]

    logger.info("Found %d pings, %d outages, %d gaps.", len(ping_lines), len(outage_lines), len(gap_lines))
    if args.dry_run:
        return 0

    episodes = outage_lines + gap_lines + meta_lines
    write_batches(influx, "bucket", episodes)
    write_batches(influx, "raw_bucket", ping_lines)
    logger.info("Imported. (Pings older than the raw bucket's retention are dropped by InfluxDB automatically.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
