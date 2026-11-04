# pidisplay forecastlog

Records weather forecasts from many models and sources in the local InfluxDB v2, so you can see
how a forecast for a given day changes as the day approaches, how accurate each source is at each
lead time, and which one is best. The InkyPi **Forecast Accuracy** plugin draws it.

It does not poll forecasts on a timer. Open-Meteo publishes tiny per-model "last run" metadata; the
daemon checks it every 10 minutes and downloads a model's forecast only when that model has produced
a new run (most models: every 6 h, HRRR: hourly).

## Install (on the Pi that runs InfluxDB)

```bash
scp -r scripts/pidisplay_forecastlog admin@pidisplay.local:/tmp/
ssh admin@pidisplay.local
sudo INFLUX_ADMIN_TOKEN=<admin token> bash /tmp/pidisplay_forecastlog/install.sh
```

The installer creates the `forecast` bucket (kept forever, it is tiny), a token scoped to it, and a
systemd service. Put that same token in InkyPi's `.env` as `INFLUXDB_FORECAST_TOKEN`.

## Sources

| tag(s) | source | horizon at London, ON |
|---|---|---|
| `gfs_global`, `ecmwf_ifs025`, `ecmwf_aifs025_single`, `icon_global`, `jma_gsm`, `ukmo_global_deterministic_10km`, `meteofrance_arpege_world` | Open-Meteo global models | 16, 14, 15, 7, 10, 6, 4 days |
| `ncep_rrfs_conus`, `gem_hrdps_continental`, `ncep_hrrr_conus` | Open-Meteo regional high-resolution models | ~3, 2, 2 days |
| `met_norway` | MET Norway locationforecast | ~8 days |
| `eccc` | Environment Canada city forecast (Canada only; needs a site id) | ~7 days |
| `owm` | OpenWeatherMap One Call 3.0 (optional; needs `api_key`) | ~8 days |

Not included: NWS (US only), Canada's GEM global model (Open-Meteo's run metadata for it is stale, so
new runs can't be detected). Models only reach as far as their horizon, and days a source only partly
covers are not stored.

MET Norway, Environment Canada and OpenWeatherMap publish no model-run metadata: the first two are
polled every `[sources] poll_minutes` and written when their own "updated" stamp changes, OpenWeatherMap
is sampled every `[owm] interval_minutes`. Their hourly or period-based data is aggregated into local
calendar days (Environment Canada's overnight low is attributed to the following calendar day; its
precipitation is the sum of the day and night periods, with a missing accumulation counted as 0).

## Adjusting

Edit `/etc/forecastlog/forecastlog.conf`; it is re-read every poll, no restart needed. Add locations in
`[locations]` (`name = lat, lon, timezone`), and an Environment Canada site id for each in `[eccc_sites]`.

```bash
sudo nano /etc/forecastlog/forecastlog.conf
sudo -u forecastlog /usr/bin/python3 /opt/forecastlog/forecastlog.py --once --force --dry-run  # print what would be written
journalctl -u forecastlog -f                                                                    # watch it
```

## Data layout (bucket `forecast`)

```
forecast,location,model,lead_h   tmax tmin precip wind wdir code run
actual,location,model            tmax tmin precip wind wdir code
```

- A `forecast` point's timestamp is the day it is **for** (12:00 UTC of that date), so queries must reach into the
  future: `range(start: -30d, stop: 20d)`.
- `lead_h` is hours from the model run to that day. Re-writing a run (long-range days sometimes arrive late)
  just overwrites the same points.
- `actual` with `model=<model>` is that model's own analysis of days already past. `actual` with
  `model=reference` is Open-Meteo's archive (reanalysis), the common yardstick used to score every model.
- Units are always metric: °C, mm, km/h (daily peak), degrees.

Error of a model at a given lead: `abs(forecast value - actual(model=reference) value)` for the same timestamp.
