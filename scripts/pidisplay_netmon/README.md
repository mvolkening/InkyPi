# pidisplay netmon

Pings the internet once a second and runs speed tests on a schedule, storing
everything in the local InfluxDB v2. The InkyPi **Internet Outage Monitor**
plugin only reads and draws this data.

## Install (on the Pi that runs InfluxDB)

```bash
scp -r scripts/pidisplay_netmon admin@pidisplay.local:/tmp/nm
ssh admin@pidisplay.local
sudo INFLUX_ADMIN_TOKEN=<admin token> bash /tmp/nm/install.sh
```

The installer installs `speedtest-cli`, creates a `netmon` user and systemd service,
creates the buckets `netmon_raw` (48 h retention) and `netmon` (400 d), and a token scoped
to just those two, which it writes into `/etc/netmon/netmon.conf`. Put that same token
in InkyPi's `.env` as `INFLUXDB_NETMON_TOKEN`.

## Adjusting

Edit `/etc/netmon/netmon.conf`; changes are picked up within seconds, no restart.
The main one is `[speedtest] interval_minutes` (default 60; each test moves a few hundred MB).

```bash
sudo nano /etc/netmon/netmon.conf
sudo /usr/bin/python3 /opt/netmon/netmon.py --speedtest-now   # run one test immediately
journalctl -u netmon -f                                        # watch it
```

## Importing old InkyPi history

Copy InkyPi's `src/plugins/network_monitor/data/pings.db` to the Pi, then:

```bash
sudo python3 /opt/netmon/migrate_from_inkypi.py /path/to/pings.db
```

Add `--dry-run` to see counts only. Re-running is harmless.
