# pidisplay nilm (appliance disaggregation)

Splits the whole-house meter's power into individual appliances, without a meter on each one
(non-intrusive load monitoring). Every switch-on or switch-off is a step in the meter's per-leg
real and reactive power, and the step's shape identifies the appliance:

| clue | what it tells |
|---|---|
| legs | both legs step together = a 240 V appliance (water heater, range, well pump); one leg = 120 V |
| real power | size in W |
| reactive power | motors and compressors (pumps, fridges, furnace blower) draw it; heating elements don't |
| run time and time of day | water heater: minutes, day and night; coffee maker plate: 15 s pulses at breakfast |

Each ON step is paired with its OFF step to make a cycle (start, run time, energy). Cycles with the
same signature get a stable id (`L002`) the first time they are seen. Name an id once you know what it
is; history is kept per id, so the name applies to everything already recorded.

It reads Node-RED's `PowerMeter` bucket (one reading per ~2 s), so it needs no new hardware and
on its first start learns from the stored history (`backfill_days`, default 120).

## Install (on pidisplay)

```bash
scp -r scripts/pidisplay_nilm admin@pidisplay.local:/tmp/
ssh admin@pidisplay.local
sudo bash /tmp/pidisplay_nilm/install.sh
sudo nano /etc/nilm/nilm.conf      # token = a READ-ONLY InfluxDB token (e.g. the ha-readonly one)
```

The token only needs read access to the `PowerMeter` and `homeassistant` buckets: results are kept in
`/var/lib/nilm/nilm.db` (SQLite) and sent to Home Assistant over MQTT. The config is re-read every
minute, so saving the token is enough; the first pass through the history takes a few minutes.

## Naming loads

```bash
sudo nilm --report                     # learned loads, with hints
sudo nilm --label L002 "Hot water tank"
sudo nilm --label L017 "Range"          # same name on several ids adds them up (range elements,
sudo nilm --label L045 "Range"          #   a water heater's upper + lower element)
sudo nilm --label L031 ignore           # hide one
sudo nilm --label L031 ""               # remove a name
```

Saw something switch? `sudo nilm --at 19:00` (or `--at "2026-11-14 19:00" --minutes 10`) lists every
step the meter recorded around that time, with the load id and name, so the right id is easy to find.

`--report` lists each recurring load with its legs, power, reactive type, typical run, runs and kWh
per day (last 14 days) and an hour-of-day profile, plus two hints:

- **looks like:** a guess from the signature alone ("240 V heating element running day and night:
  water heater?"). Only a starting point.
- **HA match:** a Home Assistant switch, plug or sensor whose on/off changes line up with the load's
  steps (e.g. `switch.pool_room_pool_light`). Plugs with their own meter and switched lights are
  found this way.

## In Home Assistant

Each named load, and the biggest recurring unnamed ones (`publish_max`), appear through MQTT
discovery as a device with **Power** (W) and **Energy** (kWh, usable in the Energy dashboard).
The power sensor's attributes carry the signature, typical run, runs per day, kWh per day/today,
first seen, the guess and the HA match. A "House loads" device adds **Identified**, **Always-on**
(the lowest 10-minute average of the last day) and **Other** power.

## Limits

- Loads that never switch (always-on electronics) only show up in "Always-on".
- Inverter compressors (many modern fridges) and variable-speed motors ramp instead of stepping,
  so they are found poorly or not at all.
- Two appliances with the same signature on the same leg share one id.
- Steps under 30 W are ignored; very small loads (LED lights, chargers) are blurry.

## Re-learning

After an algorithm change, re-learn from the stored history (load ids, and so their names, stay
attached to the same signatures):

```bash
sudo systemctl stop nilm
sudo -u nilm nilm --rebuild --since 2026-07-23
sudo systemctl start nilm
```

`journalctl -u nilm -f` shows what it is doing.
