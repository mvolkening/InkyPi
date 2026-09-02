import csv
import io
import logging
import math
import re
from datetime import date, datetime, timedelta

import pytz
import requests
from PIL import Image, ImageDraw

from plugins.base_plugin.base_plugin import BasePlugin
from utils.app_utils import get_font

logger = logging.getLogger(__name__)

DEFAULT_TITLE = "Home Power Monitor"
DEFAULT_BUCKET = "PowerMeter"
DEFAULT_VOLTAGE_A_FIELD = "VoltageA"
DEFAULT_VOLTAGE_B_FIELD = "VoltageB"
DEFAULT_FREQUENCY_FIELD = "Frequency"
DEFAULT_POWER_FACTOR_A_FIELD = "PowerFactorA"
DEFAULT_POWER_FACTOR_B_FIELD = "PowerFactorB"
DEFAULT_TOTAL_ENERGY_FIELD = "TotalEnergy"

FIELD_SUM = "PowerSum"
FIELD_A = "PowerA"
FIELD_B = "PowerB"

# Power factor is dimensionless (0-1); values below ~0.9 are the conventional
# "investigate this" threshold for reactive-heavy or failing loads. Fixed
# (not user-configurable like Voltage/Frequency's nominal) since it's a
# universal technical range, not a region-dependent grid standard.
POWER_FACTOR_MIN = 0.7
POWER_FACTOR_MAX = 1.0

# (connect, read) timeouts. Connect stays short so a genuinely unreachable
# InfluxDB fails fast; the read timeout is generous because the 30-day
# daily-`integral()` queries (fetch_daily_energy_wh / fetch_total_energy_series)
# scan a month of raw high-frequency samples and can take tens of seconds on a
# Pi-hosted InfluxDB. The plugin refresh is not latency-sensitive.
REQUEST_TIMEOUT = (5, 90)

# The sparklines always look back over a fixed trailing 24h window, downsampled
# from Influx at a fixed resolution and then bucketed further per-panel for display.
SPARKLINE_RANGE = "-24h"
SPARKLINE_AGGREGATE_EVERY = "30s"

DAILY_BAR_DAYS = 30

# Ontario Energy Board Time-of-Use schedule: fixed by regulation (only the
# three cent/kWh rates below actually change, roughly twice a year) - see
# https://www.oeb.ca/consumer-information-and-protection/electricity-rates
TOU_SUMMER_MONTHS = range(5, 11)  # May-October
TOU_MORNING_START, TOU_MIDDAY_START, TOU_EVENING_START, TOU_NIGHT_START = 7, 11, 17, 19

# Ontario's rates effective Nov 1, 2025 - Oct 31, 2026, used as fallback
# defaults so the cost chart still works even if a plugin instance was saved
# before these settings existed (the settings form itself pre-fills the same
# values). Update alongside settings.html when the OEB changes pricing.
DEFAULT_ON_PEAK_RATE = 20.3
DEFAULT_MID_PEAK_RATE = 15.7
DEFAULT_OFF_PEAK_RATE = 9.8

# Fixed 6-color e-ink palette, matching the Internet Outage Monitor plugin.
COLOR_BLACK = "#000000"
COLOR_WHITE = "#ffffff"
COLOR_RED = "#a6242b"
COLOR_YELLOW = "#dfaf2c"
COLOR_BLUE = "#345f94"
COLOR_GREEN = "#428c46"

# Shared between the daily-energy and daily-cost stacked charts so the same
# TOU period always reads as the same color in both places. Chosen to match
# the on/mid/off-peak colors utilities themselves commonly use (red/yellow/green).
PERIOD_COLORS = {"on": COLOR_RED, "mid": COLOR_YELLOW, "off": COLOR_GREEN}
PERIOD_LABELS = {"on": "On-Peak", "mid": "Mid-Peak", "off": "Off-Peak"}


class InfluxQueryError(RuntimeError):
    pass


def _flux_escape(value):
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _parse_annotated_csv(text):
    """Parses InfluxDB's "annotated CSV" query response into a list of row dicts,
    skipping '#'-prefixed annotation lines and resetting the header on each blank
    line (InfluxDB emits one header per result table)."""
    rows = []
    header = None
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


def _parse_influx_time(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=pytz.UTC)
        except ValueError:
            continue
    return None


def _parse_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_rfc3339(ts):
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ")


def _easter_sunday(year):
    """Anonymous Gregorian algorithm (Meeus/Jones/Butcher) for the date of
    Easter Sunday, needed since Good Friday is a fixed Ontario statutory
    holiday but its date moves every year."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _nth_weekday_of_month(year, month, weekday, n):
    """The date of the nth occurrence of `weekday` (Monday=0) in a given month."""
    first_day = date(year, month, 1)
    offset = (weekday - first_day.weekday()) % 7
    return date(year, month, 1 + offset + (n - 1) * 7)


def _ontario_statutory_holidays(year):
    """Ontario's statutory holidays observed for TOU billing purposes. Computed
    rather than hardcoded since several move every year (Good Friday depends on
    Easter; Family Day/Labour Day/Thanksgiving are "Nth weekday of month")."""
    return {
        date(year, 1, 1),                          # New Year's Day
        _nth_weekday_of_month(year, 2, 0, 3),       # Family Day: 3rd Monday of Feb
        _easter_sunday(year) - timedelta(days=2),   # Good Friday
        date(year, 5, 24) - timedelta(days=date(year, 5, 24).weekday()),  # Victoria Day: Monday on/before May 24
        date(year, 7, 1),                           # Canada Day
        _nth_weekday_of_month(year, 9, 0, 1),       # Labour Day: 1st Monday of Sep
        _nth_weekday_of_month(year, 10, 0, 2),      # Thanksgiving: 2nd Monday of Oct
        date(year, 12, 25),                         # Christmas Day
        date(year, 12, 26),                         # Boxing Day
    }


def _tou_period(dt_local, holiday_dates):
    """Classifies an hour as on/mid/off-peak per Ontario's TOU schedule: weekends
    and statutory holidays are off-peak all day, every day; weekdays follow a
    summer or winter pattern depending on the month."""
    if dt_local.weekday() >= 5 or dt_local.date() in holiday_dates:
        return "off"
    hour = dt_local.hour
    is_summer = dt_local.month in TOU_SUMMER_MONTHS
    if is_summer:
        if TOU_MORNING_START <= hour < TOU_MIDDAY_START:
            return "mid"
        if TOU_MIDDAY_START <= hour < TOU_EVENING_START:
            return "on"
        if TOU_EVENING_START <= hour < TOU_NIGHT_START:
            return "mid"
        return "off"
    else:
        if TOU_MORNING_START <= hour < TOU_MIDDAY_START:
            return "on"
        if TOU_MIDDAY_START <= hour < TOU_EVENING_START:
            return "mid"
        if TOU_EVENING_START <= hour < TOU_NIGHT_START:
            return "on"
        return "off"


def _sum_period_wh(hourly_means, tz, holiday_dates):
    """Buckets an hourly PowerSum series into Wh totals per (local date, TOU
    period). An hourly mean-power bucket in Watts is numerically equal to Wh
    for that hour, so no extra conversion is needed beyond dividing by 1000
    for kWh later."""
    by_day = {}
    for ts, mean_w in hourly_means:
        dt_local = datetime.fromtimestamp(ts, tz)
        period = _tou_period(dt_local, holiday_dates)
        day_bucket = by_day.setdefault(dt_local.date(), {"on": 0.0, "mid": 0.0, "off": 0.0})
        day_bucket[period] += mean_w
    return by_day


def _cost_from_period_wh(period_wh, rates_cents_per_kwh):
    if period_wh is None or any(rate is None for rate in rates_cents_per_kwh.values()):
        return None
    total = 0.0
    for period, wh in period_wh.items():
        total += (wh / 1000.0) * (rates_cents_per_kwh[period] / 100.0)
    return total


class _InfluxClient:
    """Thin wrapper around InfluxDB v2's HTTP query API (Flux over /api/v2/query),
    used instead of the official influxdb-client SDK to keep this plugin's
    footprint consistent with the rest of InkyPi's plugins (plain `requests`
    calls, no extra heavyweight dependency)."""

    def __init__(self, url, org, token, bucket, measurement, verify_ssl, voltage_a_field, voltage_b_field,
                 frequency_field, power_factor_a_field, power_factor_b_field, total_energy_field, timezone_name):
        self.url = url.rstrip("/")
        self.org = org
        self.token = token
        self.bucket = bucket
        self.measurement = measurement
        self.verify_ssl = verify_ssl
        self.voltage_a_field = voltage_a_field
        self.voltage_b_field = voltage_b_field
        self.frequency_field = frequency_field
        self.power_factor_a_field = power_factor_a_field
        self.power_factor_b_field = power_factor_b_field
        self.total_energy_field = total_energy_field
        self.timezone_name = timezone_name

    def _location_option(self):
        # Flux's aggregateWindow(every: 1d, ...) aligns day boundaries to UTC
        # midnight by default, not the caller's local calendar day - this
        # option makes "1d" buckets align to local midnight instead, which
        # matters for any of the day-granularity queries below.
        return f'import "timezone"\noption location = timezone.location(name: "{_flux_escape(self.timezone_name)}")\n\n'

    def _measurement_filter(self):
        if not self.measurement:
            return ""
        return f' and r._measurement == "{_flux_escape(self.measurement)}"'

    def _query(self, flux):
        endpoint = f"{self.url}/api/v2/query"
        headers = {
            "Authorization": f"Token {self.token}",
            "Content-Type": "application/vnd.flux",
            "Accept": "application/csv",
        }
        # InfluxDB org names are matched as an exact string (whitespace, case,
        # and punctuation all count), which is easy to get subtly wrong by hand.
        # Org IDs are unambiguous 16-character hex strings, so if the configured
        # value looks like one, send it as `orgID` instead of `org` to sidestep
        # name-matching entirely.
        org_param = {"orgID": self.org} if re.fullmatch(r"[0-9a-fA-F]{16}", self.org) else {"org": self.org}
        try:
            response = requests.post(
                endpoint,
                params=org_param,
                headers=headers,
                data=flux.encode("utf-8"),
                timeout=REQUEST_TIMEOUT,
                verify=self.verify_ssl,
            )
        except requests.RequestException as e:
            raise InfluxQueryError(f"Could not reach InfluxDB at {self.url}: {e}")

        if not 200 <= response.status_code < 300:
            raise InfluxQueryError(f"InfluxDB query failed with status {response.status_code}: {response.text[:300]}")
        return _parse_annotated_csv(response.text)

    def fetch_series(self, start):
        """Fetches PowerSum/PowerA/PowerB/VoltageA/VoltageB/Frequency/
        PowerFactorA/PowerFactorB, pivoted into one row per timestamp, over a
        relative Flux duration (e.g. "-24h")."""
        fields = [
            FIELD_SUM, FIELD_A, FIELD_B,
            self.voltage_a_field, self.voltage_b_field, self.frequency_field,
            self.power_factor_a_field, self.power_factor_b_field,
        ]
        field_conditions = " or ".join(f'r._field == "{_flux_escape(f)}"' for f in fields)
        keep_cols = ", ".join(f'"{_flux_escape(f)}"' for f in fields)
        flux = f'''from(bucket: "{_flux_escape(self.bucket)}")
  |> range(start: {start})
  |> filter(fn: (r) => ({field_conditions}){self._measurement_filter()})
  |> aggregateWindow(every: {SPARKLINE_AGGREGATE_EVERY}, fn: mean, createEmpty: false)
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", {keep_cols}])
  |> sort(columns: ["_time"])
'''
        points = []
        for row in self._query(flux):
            ts = _parse_influx_time(row.get("_time"))
            if ts is None:
                continue
            points.append((
                int(ts.timestamp()),
                _parse_float(row.get(FIELD_SUM)),
                _parse_float(row.get(FIELD_A)),
                _parse_float(row.get(FIELD_B)),
                _parse_float(row.get(self.voltage_a_field)),
                _parse_float(row.get(self.voltage_b_field)),
                _parse_float(row.get(self.frequency_field)),
                _parse_float(row.get(self.power_factor_a_field)),
                _parse_float(row.get(self.power_factor_b_field)),
            ))
        return points

    def fetch_hourly_means(self, start_ts):
        """Mean PowerSum per hour since an absolute start timestamp."""
        flux = f'''from(bucket: "{_flux_escape(self.bucket)}")
  |> range(start: {_to_rfc3339(start_ts)})
  |> filter(fn: (r) => (r._field == "{FIELD_SUM}"){self._measurement_filter()})
  |> aggregateWindow(every: 1h, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
  |> sort(columns: ["_time"])
'''
        points = []
        for row in self._query(flux):
            ts = _parse_influx_time(row.get("_time"))
            value = _parse_float(row.get("_value"))
            if ts is not None and value is not None:
                points.append((int(ts.timestamp()), value))
        return points

    def fetch_daily_energy_wh(self, start_ts):
        """Watt-hours of PowerSum per calendar day since an absolute start
        timestamp (Influx's `integral()`, wrapped so it can run inside
        `aggregateWindow`), used for the daily-energy bar chart."""
        flux = f'''{self._location_option()}from(bucket: "{_flux_escape(self.bucket)}")
  |> range(start: {_to_rfc3339(start_ts)})
  |> filter(fn: (r) => (r._field == "{FIELD_SUM}"){self._measurement_filter()})
  |> aggregateWindow(every: 1d, fn: (tables=<-, column) => tables |> integral(unit: 1h, column: column), createEmpty: false)
  |> keep(columns: ["_time", "_value"])
  |> sort(columns: ["_time"])
'''
        points = []
        for row in self._query(flux):
            ts = _parse_influx_time(row.get("_time"))
            value = _parse_float(row.get("_value"))
            if ts is not None and value is not None:
                points.append((int(ts.timestamp()), value))
        return points

    def fetch_total_energy_series(self, start_ts, every):
        """Energy consumed per bucket (day or hour) since an absolute start
        timestamp, derived from the cumulative Total Energy counter via
        Flux's `difference(nonNegative: true)`. That clips every individual
        sample-to-sample dip (a counter reset, a reboot, or just ordinary
        measurement jitter) to 0 at the raw-sample level before summing into
        buckets - which is far more robust than taking each bucket's last
        reading and diffing between buckets in Python, since one noisy sample
        can only zero out its own tiny interval instead of an entire bucket.

        The explicit `group()` collapses every series matching this field
        (e.g. if the device registered under a new tag combination after a
        reboot/reconnect, InfluxDB sees that as a separate series) back into
        one before differencing. Without it, each parallel series would be
        differenced and summed independently, and since results are combined
        by simply appending every returned row, multiple series covering the
        same time period would silently multiply the total instead of
        representing one continuous counter."""
        flux = f'''{self._location_option()}from(bucket: "{_flux_escape(self.bucket)}")
  |> range(start: {_to_rfc3339(start_ts)})
  |> filter(fn: (r) => (r._field == "{_flux_escape(self.total_energy_field)}"){self._measurement_filter()})
  |> group(columns: ["_measurement", "_field"])
  |> sort(columns: ["_time"])
  |> difference(nonNegative: true, columns: ["_value"])
  |> aggregateWindow(every: {every}, fn: sum, createEmpty: false)
  |> keep(columns: ["_time", "_value"])
  |> sort(columns: ["_time"])
'''
        points = []
        for row in self._query(flux):
            ts = _parse_influx_time(row.get("_time"))
            value = _parse_float(row.get("_value"))
            if ts is not None and value is not None:
                points.append((int(ts.timestamp()), value))
        return points


def _scale_to_wh(points, unit):
    scale = 1000.0 if unit == "kWh" else 1.0
    return [(ts, value * scale) for ts, value in points]


def _counter_unit_scale(counter_daily_wh, reference_daily_wh, tz):
    """Cross-checks daily energy derived from the meter's cumulative counter
    against the power-sample integral over the same days (the integral is
    unit-safe since it comes straight from watts). When the two disagree by
    roughly a factor of 1000, the configured Total Energy Unit doesn't match
    the meter's counter (kWh vs Wh) and every energy/cost figure would
    silently be 1000x off - so the matching correction factor is returned for
    the caller to apply. Anything else returns 1.0, trusting the counter as
    configured."""
    reference_by_date = {datetime.fromtimestamp(ts, tz).date(): wh for ts, wh in reference_daily_wh}
    counter_sum = reference_sum = 0.0
    for ts, wh in counter_daily_wh:
        reference_wh = reference_by_date.get(datetime.fromtimestamp(ts, tz).date())
        if reference_wh is not None:
            counter_sum += wh
            reference_sum += reference_wh
    if counter_sum <= 0 or reference_sum <= 0:
        return 1.0
    ratio = reference_sum / counter_sum
    if ratio >= 100.0:
        logger.warning(
            f"Total Energy counter daily totals are ~{ratio:.0f}x lower than the power-sample integral; "
            "auto-correcting by x1000 - the 'Total Energy Unit' setting likely doesn't match the meter (should be kWh)."
        )
        return 1000.0
    if ratio <= 0.01:
        logger.warning(
            f"Total Energy counter daily totals are ~{1 / ratio:.0f}x higher than the power-sample integral; "
            "auto-correcting by x0.001 - the 'Total Energy Unit' setting likely doesn't match the meter (should be Wh)."
        )
        return 0.001
    return 1.0


def _bucket_series(pairs, since_ts, until_ts, target_points):
    """Averages a (ts, value) series into ~target_points buckets across
    [since_ts, until_ts], so dense Influx samples don't draw as noise on a
    display that only has a few hundred pixel columns to work with."""
    span = max(1, until_ts - since_ts)
    target_points = max(20, target_points)
    bucket_span = max(1.0, span / target_points)

    buckets = {}
    for ts, value in pairs:
        if ts < since_ts or ts > until_ts or value is None:
            continue
        idx = int((ts - since_ts) / bucket_span)
        bucket_ts, values = buckets.get(idx, (ts, []))
        values.append(value)
        buckets[idx] = (bucket_ts, values)

    points = [(bucket_ts, sum(values) / len(values)) for bucket_ts, values in buckets.values()]
    points.sort(key=lambda p: p[0])
    return points, bucket_span


class PowerMonitor(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params['api_key'] = {
            "required": True,
            "service": "InfluxDB",
            "expected_key": "INFLUXDB_TOKEN"
        }
        return template_params

    def generate_image(self, settings, device_config):
        influx_url = (settings.get('influxUrl') or '').strip()
        influx_org = (settings.get('influxOrg') or '').strip()
        influx_bucket = (settings.get('influxBucket') or '').strip() or DEFAULT_BUCKET
        influx_measurement = (settings.get('influxMeasurement') or '').strip()
        verify_ssl = str(settings.get('skipTlsVerify', '')).lower() != 'true'
        voltage_a_field = (settings.get('voltageAField') or '').strip() or DEFAULT_VOLTAGE_A_FIELD
        voltage_b_field = (settings.get('voltageBField') or '').strip() or DEFAULT_VOLTAGE_B_FIELD
        frequency_field = (settings.get('frequencyField') or '').strip() or DEFAULT_FREQUENCY_FIELD
        power_factor_a_field = (settings.get('powerFactorAField') or '').strip() or DEFAULT_POWER_FACTOR_A_FIELD
        power_factor_b_field = (settings.get('powerFactorBField') or '').strip() or DEFAULT_POWER_FACTOR_B_FIELD
        total_energy_field = (settings.get('totalEnergyField') or '').strip() or DEFAULT_TOTAL_ENERGY_FIELD
        total_energy_unit = settings.get('totalEnergyUnit') if settings.get('totalEnergyUnit') in ('Wh', 'kWh') else 'Wh'
        use_total_energy = str(settings.get('useTotalEnergy', '')).lower() == 'true'

        if not influx_url or not influx_org:
            raise RuntimeError("InfluxDB URL and Organization are required.")

        token = device_config.load_env_key("INFLUXDB_TOKEN")
        if not token:
            raise RuntimeError("InfluxDB API token not configured.")

        title = (settings.get('title') or '').strip() or DEFAULT_TITLE
        currency_symbol = (settings.get('currencySymbol') or '').strip() or '$'
        daily_budget_kwh = self._parse_optional_float(settings.get('dailyBudgetKwh'))
        elevated_w = self._parse_int(settings.get('elevatedThresholdW'), default=1500, min_value=0, max_value=50000)
        high_w = self._parse_int(settings.get('highThresholdW'), default=3000, min_value=0, max_value=50000)
        nominal_voltage = self._parse_optional_float(settings.get('nominalVoltage')) or 120.0
        nominal_frequency = self._parse_optional_float(settings.get('nominalFrequency')) or 60.0

        cost_model = settings.get('costModel') if settings.get('costModel') in ('flat', 'tou') else 'flat'
        price_per_kwh = self._parse_optional_float(settings.get('pricePerKwh'))
        tou_rates = {
            "on": self._parse_optional_float(settings.get('onPeakRate')) or DEFAULT_ON_PEAK_RATE,
            "mid": self._parse_optional_float(settings.get('midPeakRate')) or DEFAULT_MID_PEAK_RATE,
            "off": self._parse_optional_float(settings.get('offPeakRate')) or DEFAULT_OFF_PEAK_RATE,
        }
        extra_holiday_dates = self._parse_holiday_dates(settings.get('statutoryHolidays'))

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        timezone_name = device_config.get_config("timezone", default="America/New_York")
        time_format = device_config.get_config("time_format", default="12h")
        tz = pytz.timezone(timezone_name)
        now_dt = datetime.now(tz)
        now_ts = int(now_dt.timestamp())
        today = now_dt.date()

        client = _InfluxClient(
            influx_url, influx_org, token, influx_bucket, influx_measurement, verify_ssl,
            voltage_a_field, voltage_b_field, frequency_field,
            power_factor_a_field, power_factor_b_field, total_energy_field, timezone_name,
        )

        # Ontario's statutory holidays are computed automatically (they're a
        # fixed, regulation-defined list, several of which move every year) so
        # off-peak billing is accurate without the user having to type or
        # maintain the list themselves. The 30-day lookback window can cross a
        # year boundary, so holidays are computed for the surrounding years too.
        # `statutoryHolidays` adds any extra dates (e.g. a company shutdown day)
        # on top of the automatic list.
        holiday_dates = extra_holiday_dates.union(*(_ontario_statutory_holidays(y) for y in (today.year - 1, today.year, today.year + 1)))

        try:
            series = client.fetch_series(SPARKLINE_RANGE)

            month_start_date = today - timedelta(days=DAILY_BAR_DAYS - 1)
            month_start_ts = int(tz.localize(datetime.combine(month_start_date, datetime.min.time())).timestamp())

            # The meter's own cumulative Total Energy counter is opt-in (see
            # `useTotalEnergy`): in principle it's immune to sampling gaps/noise
            # that integrating power samples is prone to, but that's only true
            # if the counter itself is trustworthy - a stuck/corrupted readout
            # can silently inflate results, so this stays off until a user
            # explicitly enables it, falling back to the power-sample estimate
            # whenever it's off or has no usable data.
            daily_energy = []
            # Correction factor for a counter whose unit doesn't match the
            # `totalEnergyUnit` setting (detected by _counter_unit_scale);
            # shared by the daily and hourly counter series so both stay on
            # the same scale. None means "counter not in use / no counter data".
            energy_scale = None
            if use_total_energy:
                total_energy_daily_raw = client.fetch_total_energy_series(month_start_ts, "1d")
                counter_daily = _scale_to_wh(total_energy_daily_raw, total_energy_unit)
                if counter_daily:
                    reference_daily = client.fetch_daily_energy_wh(month_start_ts)
                    energy_scale = _counter_unit_scale(counter_daily, reference_daily, tz)
                    daily_energy = [(ts, wh * energy_scale) for ts, wh in counter_daily]
            if not daily_energy:
                daily_energy = client.fetch_daily_energy_wh(month_start_ts)

            # The hourly-resolution query (needed to split energy/cost by TOU
            # period) is only fetched when it'll actually be used, since flat-rate
            # billing has no use for a per-hour breakdown.
            day_period_wh = {}
            if cost_model == "tou":
                hourly_energy = []
                if energy_scale is not None:
                    total_energy_hourly_raw = client.fetch_total_energy_series(month_start_ts, "1h")
                    hourly_energy = [(ts, wh * energy_scale) for ts, wh in _scale_to_wh(total_energy_hourly_raw, total_energy_unit)]
                if not hourly_energy:
                    hourly_energy = client.fetch_hourly_means(month_start_ts)
                day_period_wh = _sum_period_wh(hourly_energy, tz, holiday_dates)
        except InfluxQueryError as e:
            logger.error(f"Failed to query InfluxDB: {str(e)}")
            raise RuntimeError("Failed to retrieve data from InfluxDB, please check logs.")

        if not series:
            raise RuntimeError("No power data returned from InfluxDB for the last 24 hours. Check the bucket/field names and that the meter is reporting.")

        width, height = dimensions
        fonts = {
            "title": get_font("Jost", round(height * 0.040), "bold"),
            "stat": get_font("Jost", round(height * 0.026), "bold"),
            "label": get_font("Jost", round(height * 0.020), "bold"),
            "small": get_font("Jost", round(height * 0.014), "normal"),
        }

        image = Image.new("RGB", dimensions, COLOR_WHITE)
        draw = ImageDraw.Draw(image)

        title_height = round(height * 0.075)
        stats_height = round(height * 0.075)

        stats = self._compute_stats(series, daily_energy, day_period_wh, now_dt, cost_model, price_per_kwh, tou_rates)
        if cost_model == "tou":
            status_label, status_color = self._current_tou_status(now_dt, holiday_dates)
        else:
            status_label, status_color = self._status(stats["current_power"], elevated_w, high_w)

        self._draw_title(draw, width, title_height, title, fonts, status_label, status_color, series[-1][0], tz, time_format)
        self._draw_stats_bar(draw, width, title_height, stats_height, fonts, stats, currency_symbol)

        margin = round(width * 0.012)
        body_top = title_height + stats_height
        left_width = round(width * 2 / 3)

        left_box = (margin, body_top + margin, left_width - margin // 2, height - margin)
        right_left = left_width + margin // 2
        right_right = width - margin
        right_top = body_top + margin
        right_bottom = height - margin
        right_height = right_bottom - right_top
        gauge_gap = margin

        right_col_width = right_right - right_left
        landscape_layout = right_col_width >= 220
        frequency_spark_box = power_factor_spark_box = None
        if landscape_layout:
            # Voltage keeps the full width (its dual A/B readout needs the
            # room); Frequency and Power Factor - each a single-needle gauge
            # with a short readout - share the row below it, and (only here,
            # where there's enough spare width) each gets a small 6h trend
            # sparkline underneath to fill the leftover vertical space.
            voltage_box = (right_left, right_top, right_right, right_top + round(right_height * 0.46))
            bottom_row_top = right_top + round(right_height * 0.46) + gauge_gap
            half_w = round((right_col_width - gauge_gap) / 2)
            bottom_row_h = right_bottom - bottom_row_top
            gauge_h = round(bottom_row_h * 0.58)
            spark_top = bottom_row_top + gauge_h + round(gauge_gap * 0.6)

            frequency_box = (right_left, bottom_row_top, right_left + half_w, bottom_row_top + gauge_h)
            frequency_spark_box = (right_left, spark_top, right_left + half_w, right_bottom)
            power_factor_box = (right_left + half_w + gauge_gap, bottom_row_top, right_right, bottom_row_top + gauge_h)
            power_factor_spark_box = (right_left + half_w + gauge_gap, spark_top, right_right, right_bottom)
        else:
            # The right column is too narrow (e.g. portrait orientation) to
            # split Frequency/Power Factor side-by-side without squeezing them
            # into an unreadable sliver - stack all three gauges instead, with
            # no room left over for the extra trend sparklines.
            box_h = round((right_height - 2 * gauge_gap) / 3)
            voltage_box = (right_left, right_top, right_right, right_top + box_h)
            frequency_box = (right_left, right_top + box_h + gauge_gap, right_right, right_top + 2 * box_h + gauge_gap)
            power_factor_box = (right_left, right_top + 2 * (box_h + gauge_gap), right_right, right_bottom)

        day_period_kwh = {day: {p: wh / 1000.0 for p, wh in periods.items()} for day, periods in day_period_wh.items()}
        day_period_dollars = None
        if cost_model == "tou" and all(rate is not None for rate in tou_rates.values()):
            day_period_dollars = {
                day: {p: (wh / 1000.0) * (tou_rates[p] / 100.0) for p, wh in periods.items()}
                for day, periods in day_period_wh.items()
            }

        self._draw_left_column(
            draw, left_box, series, daily_energy, day_period_kwh, day_period_dollars,
            month_start_date, today, now_ts, tz, fonts, time_format, daily_budget_kwh, cost_model, currency_symbol,
        )

        voltage_range = (nominal_voltage * 0.9, nominal_voltage * 1.1)
        frequency_range = (nominal_frequency - 2.0, nominal_frequency + 2.0)
        voltage_readings = [("A", stats["voltage_a"], COLOR_GREEN), ("B", stats["voltage_b"], COLOR_YELLOW)]
        frequency_readings = [(None, stats["frequency"], COLOR_BLACK)]
        power_factor_readings = [("A", stats["power_factor_a"], COLOR_GREEN), ("B", stats["power_factor_b"], COLOR_YELLOW)]

        self._draw_gauge(draw, voltage_box, "Voltage", voltage_readings, voltage_range[0], voltage_range[1], "V", fonts, decimals=1)
        self._draw_gauge(draw, frequency_box, "Frequency", frequency_readings, frequency_range[0], frequency_range[1], "Hz", fonts, decimals=2)
        # Power factor is bad only at the low end (it maxes out at 1.0, which
        # is ideal, not "too high"), so it gets an ascending red->yellow->green
        # band pattern instead of the symmetric one used for Voltage/Frequency.
        self._draw_gauge(
            draw, power_factor_box, "Power Factor", power_factor_readings,
            POWER_FACTOR_MIN, POWER_FACTOR_MAX, "", fonts, decimals=2, scale_decimals=1,
            bands=[(0.0, 0.5, COLOR_RED), (0.5, 0.833, COLOR_YELLOW), (0.833, 1.0, COLOR_GREEN)],
        )

        if frequency_spark_box is not None:
            spark_since_ts = now_ts - 6 * 3600
            freq_pairs = [(ts, v) for ts, _, _, _, _, _, v, _, _ in series if v is not None]
            pf_a_pairs = [(ts, v) for ts, _, _, _, _, _, _, v, _ in series if v is not None]
            pf_b_pairs = [(ts, v) for ts, _, _, _, _, _, _, _, v in series if v is not None]

            self._draw_metric_sparkline(
                draw, frequency_spark_box, "Frequency (6h)", [(COLOR_BLACK, freq_pairs)],
                spark_since_ts, now_ts, fonts, tz, time_format, decimals=2,
            )
            self._draw_metric_sparkline(
                draw, power_factor_spark_box, "Power Factor (6h)", [(COLOR_GREEN, pf_a_pairs), (COLOR_YELLOW, pf_b_pairs)],
                spark_since_ts, now_ts, fonts, tz, time_format, decimals=2,
            )

        return image

    # ---- settings parsing -------------------------------------------------

    @staticmethod
    def _parse_int(value, default, min_value, max_value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = default
        return max(min_value, min(max_value, value))

    @staticmethod
    def _parse_optional_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_holiday_dates(value):
        dates = set()
        for part in (value or "").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                dates.add(datetime.strptime(part, "%Y-%m-%d").date())
            except ValueError:
                logger.warning(f"Ignoring invalid statutory holiday date: {part!r} (expected YYYY-MM-DD)")
        return dates

    # ---- derived stats ------------------------------------------------------

    def _compute_stats(self, series, daily_energy, day_period_wh, now_dt, cost_model, price_per_kwh, tou_rates):
        _, latest_sum, _, _, latest_voltage_a, latest_voltage_b, latest_freq, latest_pf_a, latest_pf_b = series[-1]
        current_power = latest_sum if latest_sum is not None else 0.0

        peak_power = max((s for _, s, _, _, _, _, _, _, _ in series if s is not None), default=current_power)

        a_values = [a for _, _, a, _, _, _, _, _, _ in series if a is not None]
        b_values = [b for _, _, _, b, _, _, _, _, _ in series if b is not None]
        phase_a_pct = phase_b_pct = None
        if a_values and b_values:
            avg_a, avg_b = sum(a_values) / len(a_values), sum(b_values) / len(b_values)
            total = avg_a + avg_b
            if total > 0:
                phase_a_pct = round(100 * avg_a / total)
                phase_b_pct = round(100 * avg_b / total)

        today_date = now_dt.date()
        today_wh = None
        for ts, wh in daily_energy:
            if datetime.fromtimestamp(ts, now_dt.tzinfo).date() == today_date:
                today_wh = wh
        today_kwh = (today_wh / 1000.0) if today_wh is not None else None

        if cost_model == "tou":
            estimated_cost = _cost_from_period_wh(day_period_wh.get(today_date), tou_rates)
        else:
            estimated_cost = today_kwh * price_per_kwh if (today_kwh is not None and price_per_kwh is not None) else None

        return {
            "current_power": current_power,
            "peak_power": peak_power,
            "phase_a_pct": phase_a_pct,
            "phase_b_pct": phase_b_pct,
            "today_kwh": today_kwh,
            "estimated_cost": estimated_cost,
            "voltage_a": latest_voltage_a,
            "voltage_b": latest_voltage_b,
            "frequency": latest_freq,
            "power_factor_a": latest_pf_a,
            "power_factor_b": latest_pf_b,
        }

    @staticmethod
    def _status(current_power, elevated_w, high_w):
        if current_power >= high_w:
            return "High", COLOR_RED
        if current_power >= elevated_w:
            return "Elevated", COLOR_YELLOW
        return "Normal", COLOR_GREEN

    @staticmethod
    def _current_tou_status(now_dt, holiday_dates):
        # Under Time-of-Use billing, which pricing period you're in right now
        # is more actionable at a glance than a raw wattage threshold - and it
        # reuses the same period colors as the two stacked-by-period charts.
        period = _tou_period(now_dt, holiday_dates)
        return PERIOD_LABELS[period], PERIOD_COLORS[period]

    # ---- rendering ------------------------------------------------------

    def _format_clock_time(self, dt, time_format):
        if time_format == "24h":
            return dt.strftime("%H:%M")
        return dt.strftime("%I:%M %p").lstrip("0")

    def _draw_title(self, draw, width, title_height, title, fonts, status_label, status_color, latest_ts, tz, time_format):
        pad = round(width * 0.0125)
        draw.line([(0, title_height), (width, title_height)], fill=COLOR_BLACK, width=3)
        draw.text((pad, title_height / 2), title, font=fonts["title"], fill=COLOR_BLACK, anchor="lm")

        badge_font = fonts["label"]
        text_w = draw.textlength(status_label, font=badge_font)
        badge_pad_x, badge_pad_y = 20, 10
        badge_w = text_w + badge_pad_x * 2
        badge_h = badge_font.size + badge_pad_y * 2
        badge_right = width - pad
        badge_left = badge_right - badge_w
        badge_top = (title_height - badge_h) / 2
        badge_bottom = badge_top + badge_h
        draw.rectangle([badge_left, badge_top, badge_right, badge_bottom], fill=status_color)
        text_color = COLOR_WHITE if status_color == COLOR_RED else COLOR_BLACK
        draw.text(
            ((badge_left + badge_right) / 2, (badge_top + badge_bottom) / 2),
            status_label,
            font=badge_font,
            fill=text_color,
            anchor="mm",
        )

        updated_dt = datetime.fromtimestamp(latest_ts, tz)
        updated_str = f"Updated {self._format_clock_time(updated_dt, time_format)}"
        draw.text((badge_left - pad, title_height / 2), updated_str, font=fonts["small"], fill=COLOR_BLACK, anchor="rm")

    def _draw_stats_bar(self, draw, width, title_height, stats_height, fonts, stats, currency_symbol):
        top = title_height
        bottom = title_height + stats_height
        draw.line([(0, bottom), (width, bottom)], fill=COLOR_BLACK, width=2)

        items = []
        if stats["today_kwh"] is not None:
            items.append(("Today", f"{stats['today_kwh']:.1f} kWh"))
        items.append(("Peak (24h)", f"{stats['peak_power']:.0f} W"))
        if stats["phase_a_pct"] is not None:
            items.append(("Leg A / B", f"{stats['phase_a_pct']}% / {stats['phase_b_pct']}%"))
        if stats["estimated_cost"] is not None:
            items.append(("Est. Cost Today", f"{currency_symbol}{stats['estimated_cost']:.2f}"))
        if not items:
            return

        col_w = width / len(items)

        # On narrow (portrait) displays the base stat/label fonts can be wider
        # than a column - shrink both (proportionally, keeping their relative
        # size) down to whatever actually fits the longest value/label.
        stat_font, label_font = fonts["stat"], fonts["small"]
        max_text_w = max(
            max(draw.textlength(value, font=stat_font) for _, value in items),
            max(draw.textlength(label, font=label_font) for label, _ in items),
        )
        available_w = col_w * 0.9
        if max_text_w > available_w and max_text_w > 0:
            scale = available_w / max_text_w
            stat_font = get_font("Jost", max(8, round(stat_font.size * scale)), "bold")
            label_font = get_font("Jost", max(7, round(label_font.size * scale)), "normal")

        for i, (label, value) in enumerate(items):
            cx = col_w * i + col_w / 2
            draw.text((cx, top + stats_height * 0.32), value, font=stat_font, fill=COLOR_BLACK, anchor="mm")
            draw.text((cx, top + stats_height * 0.72), label, font=label_font, fill=COLOR_BLUE, anchor="mm")
            if i > 0:
                draw.line([(col_w * i, top + stats_height * 0.15), (col_w * i, bottom - stats_height * 0.15)], fill=COLOR_BLACK, width=1)

    def _draw_left_column(self, draw, box, series, daily_energy, day_period_kwh, day_period_dollars,
                           month_start_date, today, now_ts, tz, fonts, time_format, daily_budget_kwh, cost_model, currency_symbol):
        x0, y0, x1, y1 = box
        gap = round((y1 - y0) * 0.03)

        # Time-of-Use billing needs two extra stacked-by-period charts (kWh and
        # $), so the two sparklines give up a bit of height to make room; flat
        # billing keeps the original 3-equal-panel layout.
        weights = [0.25, 0.25, 0.25, 0.25] if cost_model == "tou" else [1 / 3, 1 / 3, 1 / 3]
        total_gap = gap * (len(weights) - 1)
        available_h = (y1 - y0) - total_gap
        heights = [available_h * w for w in weights]
        tops = []
        cursor = y0
        for h in heights:
            tops.append(cursor)
            cursor += h + gap

        windows = [("Last 6 Hours", 6 * 3600), ("Last 24 Hours", 24 * 3600)]
        sum_pairs = [(ts, v) for ts, v, _, _, _, _, _, _, _ in series if v is not None]
        a_pairs = [(ts, v) for ts, _, v, _, _, _, _, _, _ in series if v is not None]
        b_pairs = [(ts, v) for ts, _, _, v, _, _, _, _, _ in series if v is not None]

        for i, (label, span) in enumerate(windows):
            top, bottom = tops[i], tops[i] + heights[i]
            since_ts = now_ts - span
            self._draw_sparkline_panel(
                draw, (x0, top, x1, bottom), label, since_ts, now_ts,
                sum_pairs, a_pairs, b_pairs, fonts, tz, time_format,
                show_legend=(i == 0),
            )

        if cost_model == "tou":
            self._draw_stacked_period_chart(
                draw, (x0, tops[2], x1, tops[2] + heights[2]),
                f"Daily Energy by Period (Last {DAILY_BAR_DAYS} Days)",
                day_period_kwh, month_start_date, today, fonts,
                value_fmt=lambda v: f"{v:.0f}kWh", show_legend=True,
            )
            self._draw_stacked_period_chart(
                draw, (x0, tops[3], x1, tops[3] + heights[3]),
                f"Daily Cost by Period (Last {DAILY_BAR_DAYS} Days)",
                day_period_dollars or {}, month_start_date, today, fonts,
                value_fmt=lambda v: f"{currency_symbol}{v:.0f}", show_legend=False,
            )
        else:
            self._draw_daily_bar_chart(draw, (x0, tops[2], x1, tops[2] + heights[2]), daily_energy, month_start_date, today, tz, fonts, daily_budget_kwh)

    def _draw_sparkline_panel(self, draw, box, label, since_ts, until_ts, sum_pairs, a_pairs, b_pairs, fonts, tz, time_format, show_legend):
        x0, y0, x1, y1 = box
        pad = 8
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)

        # The legend gets its own row below the panel label (rather than a
        # fixed right-side offset on the same line) so it still fits on
        # narrow/portrait displays instead of overlapping the label.
        legend_row_h = (fonts["small"].size + 6) if show_legend else 0
        chart_top = y0 + fonts["label"].size + pad * 3 + legend_row_h
        chart_bottom = y1 - pad * 2 - fonts["small"].size - 6
        chart_left = x0 + pad + 48
        chart_right = x1 - pad

        span = max(1, until_ts - since_ts)
        chart_width_px = max(1.0, chart_right - chart_left)
        target_points = max(20, int(chart_width_px // 2))

        sum_points, bucket_span = _bucket_series(sum_pairs, since_ts, until_ts, target_points)
        a_points, _ = _bucket_series(a_pairs, since_ts, until_ts, target_points)
        b_points, _ = _bucket_series(b_pairs, since_ts, until_ts, target_points)

        def x_of(ts):
            ts = min(max(ts, since_ts), until_ts)
            return chart_left + (ts - since_ts) / span * (chart_right - chart_left)

        all_values = [v for _, v in sum_points + a_points + b_points]
        y_max = max(all_values) if all_values else 1.0
        y_max = y_max if y_max > 0 else 1.0

        def y_of(value):
            value = min(value, y_max)
            return chart_bottom - (value / y_max) * (chart_bottom - chart_top)

        for frac in (0.25, 0.5, 0.75):
            gy = chart_bottom - frac * (chart_bottom - chart_top)
            draw.line([(chart_left, gy), (chart_right, gy)], fill=COLOR_BLACK, width=1)

        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)

        def draw_line(points, color, width):
            prev = None
            for ts, value in points:
                if prev is not None and ts - prev[0] <= bucket_span * 2.5:
                    draw.line([(x_of(prev[0]), y_of(prev[1])), (x_of(ts), y_of(value))], fill=color, width=width)
                prev = (ts, value)

        draw_line(a_points, COLOR_GREEN, 3)
        draw_line(b_points, COLOR_YELLOW, 3)
        draw_line(sum_points, COLOR_BLUE, 5)

        draw.text((x0 + pad, y0 + pad), label, font=fonts["label"], fill=COLOR_BLACK, anchor="la")
        if show_legend:
            legend_font = fonts["small"]
            swatch = legend_font.size
            legend_y = y0 + pad * 2 + fonts["label"].size
            lx = x0 + pad
            for name, color in (("Total", COLOR_BLUE), ("Leg A", COLOR_GREEN), ("Leg B", COLOR_YELLOW)):
                draw.rectangle([lx, legend_y, lx + swatch, legend_y + swatch], fill=color, outline=COLOR_BLACK)
                draw.text((lx + swatch + 4, legend_y), name, font=legend_font, fill=COLOR_BLACK, anchor="la")
                lx += swatch + 6 + draw.textlength(name, font=legend_font) + 14

        draw.text((x0 + pad, chart_top), f"{int(y_max)}W", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, chart_bottom), "0W", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        tick_y = chart_bottom + 4
        for frac, h_anchor in ((0.0, "l"), (0.25, "m"), (0.5, "m"), (0.75, "m"), (1.0, "r")):
            tick_ts = since_ts + frac * span
            tick_x = x_of(tick_ts)
            draw.line([(tick_x, chart_bottom), (tick_x, chart_bottom + 3)], fill=COLOR_BLACK, width=1)
            tick_dt = datetime.fromtimestamp(tick_ts, tz)
            draw.text((tick_x, tick_y), self._format_clock_time(tick_dt, time_format), font=fonts["small"], fill=COLOR_BLACK, anchor=h_anchor + "a")

    def _draw_metric_sparkline(self, draw, box, title, lines, since_ts, until_ts, fonts, tz, time_format, decimals):
        """A compact trend chart for a small leftover-space panel (e.g. under
        the Frequency/Power Factor gauges): one or more (color, pairs) lines
        sharing a single auto-scaled y-axis, with just a min/max label and
        start/end time ticks rather than the full gridlines/legend the main
        power sparklines use, since there isn't room for those here."""
        x0, y0, x1, y1 = box
        pad = 6
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)

        label_font = fonts["small"]
        draw.text((x0 + pad, y0 + pad // 2), title, font=label_font, fill=COLOR_BLACK, anchor="la")

        chart_top = y0 + label_font.size + pad * 2
        chart_bottom = y1 - pad - label_font.size - 4
        chart_left = x0 + pad + 30
        chart_right = x1 - pad

        span = max(1, until_ts - since_ts)
        chart_width_px = max(1.0, chart_right - chart_left)
        target_points = max(15, int(chart_width_px // 3))

        bucketed = []
        all_values = []
        bucket_span = 1.0
        for color, pairs in lines:
            points, bucket_span = _bucket_series(pairs, since_ts, until_ts, target_points)
            bucketed.append((color, points))
            all_values.extend(v for _, v in points)

        if not all_values:
            draw.text(((chart_left + chart_right) / 2, (chart_top + chart_bottom) / 2), "No Data", font=label_font, fill=COLOR_BLACK, anchor="mm")
            return

        y_min, y_max = min(all_values), max(all_values)
        if y_max - y_min < 1e-9:
            y_min, y_max = y_min - 0.5, y_max + 0.5
        y_span = y_max - y_min

        def x_of(ts):
            ts = min(max(ts, since_ts), until_ts)
            return chart_left + (ts - since_ts) / span * (chart_right - chart_left)

        def y_of(value):
            value = min(max(value, y_min), y_max)
            return chart_bottom - (value - y_min) / y_span * (chart_bottom - chart_top)

        mid_y = chart_bottom - 0.5 * (chart_bottom - chart_top)
        draw.line([(chart_left, mid_y), (chart_right, mid_y)], fill=COLOR_BLACK, width=1)
        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)

        for color, points in bucketed:
            prev = None
            for ts, value in points:
                if prev is not None and ts - prev[0] <= bucket_span * 2.5:
                    draw.line([(x_of(prev[0]), y_of(prev[1])), (x_of(ts), y_of(value))], fill=color, width=2)
                prev = (ts, value)

        draw.text((x0 + pad, chart_top), f"{y_max:.{decimals}f}", font=label_font, fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, chart_bottom), f"{y_min:.{decimals}f}", font=label_font, fill=COLOR_BLACK, anchor="lm")

        tick_y = chart_bottom + 4
        for tick_ts, h_anchor in ((since_ts, "l"), (until_ts, "r")):
            tick_x = x_of(tick_ts)
            tick_dt = datetime.fromtimestamp(tick_ts, tz)
            draw.text((tick_x, tick_y), self._format_clock_time(tick_dt, time_format), font=label_font, fill=COLOR_BLACK, anchor=h_anchor + "a")

    def _draw_daily_bar_chart(self, draw, box, daily_energy, start_date, today, tz, fonts, daily_budget_kwh):
        x0, y0, x1, y1 = box
        pad = 8
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), f"Daily Energy (Last {DAILY_BAR_DAYS} Days)", font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        chart_top = y0 + fonts["label"].size + pad * 3
        chart_bottom = y1 - pad * 2 - fonts["small"].size - 6
        chart_left = x0 + pad + 40
        chart_right = x1 - pad

        kwh_by_date = {}
        for ts, wh in daily_energy:
            d = datetime.fromtimestamp(ts, tz).date()
            kwh_by_date[d] = wh / 1000.0

        # Color thresholds: an explicit daily budget if the user set one,
        # otherwise auto-scaled relative to this home's own trailing average so
        # the chart is meaningful without the user having to guess a number.
        past_values = [kwh for d, kwh in kwh_by_date.items() if d != today]
        baseline = daily_budget_kwh if daily_budget_kwh else ((sum(past_values) / len(past_values)) if past_values else None)
        low_threshold = baseline * 0.85 if baseline else None
        high_threshold = baseline * 1.15 if baseline else None

        day_values = [(start_date + timedelta(days=i), kwh_by_date.get(start_date + timedelta(days=i))) for i in range(DAILY_BAR_DAYS)]
        max_value = max((kwh for _, kwh in day_values if kwh is not None), default=0.0) or 1.0

        bar_area_w = chart_right - chart_left
        bar_w = bar_area_w / DAILY_BAR_DAYS
        bar_gap = max(1.0, bar_w * 0.15)

        for i, (day_date, kwh) in enumerate(day_values):
            bx0 = chart_left + i * bar_w
            bx1 = bx0 + max(bar_w - bar_gap, 1.0)
            if kwh is not None:
                color = COLOR_BLUE
                if low_threshold is not None:
                    if kwh <= low_threshold:
                        color = COLOR_GREEN
                    elif kwh <= high_threshold:
                        color = COLOR_YELLOW
                    else:
                        color = COLOR_RED
                bar_h = (kwh / max_value) * (chart_bottom - chart_top)
                draw.rectangle([bx0, chart_bottom - bar_h, bx1, chart_bottom], fill=color)
            if day_date == today:
                draw.rectangle([bx0 - 1, chart_top, bx1 + 1, chart_bottom], outline=COLOR_BLACK, width=2)

        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)
        draw.text((x0 + pad, chart_top), f"{max_value:.0f}kWh", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, chart_bottom), "0", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        label_every = max(1, DAILY_BAR_DAYS // 6)
        tick_y = chart_bottom + 4
        for i in range(0, DAILY_BAR_DAYS, label_every):
            day_date = start_date + timedelta(days=i)
            dx = chart_left + i * bar_w + bar_w / 2
            draw.text((dx, tick_y), str(day_date.day), font=fonts["small"], fill=COLOR_BLACK, anchor="ma")

    def _draw_stacked_period_chart(self, draw, box, title, day_values, start_date, today, fonts, value_fmt, show_legend):
        x0, y0, x1, y1 = box
        pad = 8
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), title, font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        # These panels are short (they share the left column 4 ways with the two
        # sparklines), so the legend rides on the title row itself, right-aligned,
        # rather than claiming a whole extra row of already-scarce vertical space.
        if show_legend:
            legend_font = fonts["small"]
            swatch = legend_font.size
            gap = 12
            items = [("Off", PERIOD_COLORS["off"]), ("Mid", PERIOD_COLORS["mid"]), ("On", PERIOD_COLORS["on"])]
            item_widths = [swatch + 4 + draw.textlength(name, font=legend_font) for name, _ in items]
            total_w = sum(item_widths) + gap * (len(items) - 1)
            lx = x1 - pad - total_w
            title_w = draw.textlength(title, font=fonts["label"])
            # On narrow (portrait) panels the title can be wider than the space
            # left for a right-aligned legend - rather than overlap the two,
            # just skip the legend there; the shared color coding is still
            # explained by the panel below it in that layout.
            if lx > x0 + pad + title_w + 16:
                ly = y0 + pad + (fonts["label"].size - swatch) / 2
                for (name, color), item_w in zip(items, item_widths):
                    draw.rectangle([lx, ly, lx + swatch, ly + swatch], fill=color, outline=COLOR_BLACK)
                    draw.text((lx + swatch + 4, ly - 1), name, font=legend_font, fill=COLOR_BLACK, anchor="la")
                    lx += item_w + gap

        chart_top = y0 + fonts["label"].size + pad * 3
        chart_bottom = y1 - pad * 2 - fonts["small"].size - 6
        chart_left = x0 + pad + 40
        chart_right = x1 - pad

        day_totals = [(start_date + timedelta(days=i), day_values.get(start_date + timedelta(days=i))) for i in range(DAILY_BAR_DAYS)]
        max_value = max((sum(parts.values()) for _, parts in day_totals if parts), default=0.0) or 1.0

        bar_area_w = chart_right - chart_left
        bar_w = bar_area_w / DAILY_BAR_DAYS
        bar_gap = max(1.0, bar_w * 0.15)

        for i, (day_date, parts) in enumerate(day_totals):
            bx0 = chart_left + i * bar_w
            bx1 = bx0 + max(bar_w - bar_gap, 1.0)
            if parts:
                y_cursor = chart_bottom
                for period in ("off", "mid", "on"):
                    value = parts.get(period, 0.0)
                    if value <= 0:
                        continue
                    seg_h = (value / max_value) * (chart_bottom - chart_top)
                    draw.rectangle([bx0, y_cursor - seg_h, bx1, y_cursor], fill=PERIOD_COLORS[period])
                    y_cursor -= seg_h
            if day_date == today:
                draw.rectangle([bx0 - 1, chart_top, bx1 + 1, chart_bottom], outline=COLOR_BLACK, width=2)

        draw.line([(chart_left, chart_bottom), (chart_right, chart_bottom)], fill=COLOR_BLACK, width=1)
        draw.text((x0 + pad, chart_top), value_fmt(max_value), font=fonts["small"], fill=COLOR_BLACK, anchor="lm")
        draw.text((x0 + pad, chart_bottom), "0", font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        label_every = max(1, DAILY_BAR_DAYS // 6)
        tick_y = chart_bottom + 4
        for i in range(0, DAILY_BAR_DAYS, label_every):
            day_date = start_date + timedelta(days=i)
            dx = chart_left + i * bar_w + bar_w / 2
            draw.text((dx, tick_y), str(day_date.day), font=fonts["small"], fill=COLOR_BLACK, anchor="ma")

    def _draw_gauge(self, draw, box, title, readings, min_value, max_value, unit, fonts, decimals, bands=None, scale_decimals=0):
        x0, y0, x1, y1 = box
        pad = 10
        draw.rectangle([x0, y0, x1, y1], outline=COLOR_BLACK, width=2)
        draw.text((x0 + pad, y0 + pad), title, font=fonts["label"], fill=COLOR_BLACK, anchor="la")

        area_top = y0 + fonts["label"].size + pad * 3
        area_bottom = y1 - pad
        area_left = x0 + pad
        area_right = x1 - pad

        # Multi-reading gauges may need to fall back to two stacked lines on
        # narrow displays, so they reserve room for that up front rather than
        # assuming a single line always fits.
        readout_h = (fonts["stat"].size + pad) if len(readings) <= 1 else (fonts["small"].size * 2 + pad * 3)
        arc_area_h = (area_bottom - readout_h) - area_top

        # The min/max scale labels are drawn just outside the arc's two ends
        # (anchored away from center), so the radius needs to leave room for
        # their text width too - otherwise they spill past the box border.
        min_label, max_label = f"{min_value:.{scale_decimals}f}", f"{max_value:.{scale_decimals}f}"
        label_margin = max(draw.textlength(min_label, font=fonts["small"]), draw.textlength(max_label, font=fonts["small"])) + 6
        radius = max(10.0, min((area_right - area_left) / 2 - label_margin, arc_area_h))

        cx = (area_left + area_right) / 2
        cy = area_top + radius

        track_width = max(8, round(radius * 0.16))
        # Bands default to symmetric-around-the-midpoint (Voltage/Frequency,
        # whose min/max are always nominal +/- a tolerance, so "too high" and
        # "too low" are equally bad); callers with an asymmetric good/bad
        # range (e.g. Power Factor, which is only bad at the low end) pass
        # their own band list instead.
        if bands is None:
            bands = [(0.0, 0.15, COLOR_RED), (0.15, 0.85, COLOR_GREEN), (0.85, 1.0, COLOR_RED)]

        def frac_to_angle(frac):
            return 180 + max(0.0, min(1.0, frac)) * 180

        bbox = [cx - radius, cy - radius, cx + radius, cy + radius]
        for f0, f1, color in bands:
            draw.arc(bbox, frac_to_angle(f0), frac_to_angle(f1), fill=color, width=track_width)

        draw.text((cx - radius, cy + 4), min_label, font=fonts["small"], fill=COLOR_BLACK, anchor="rm")
        draw.text((cx + radius, cy + 4), max_label, font=fonts["small"], fill=COLOR_BLACK, anchor="lm")

        span = max(1e-9, max_value - min_value)
        needle_len = radius * 0.8
        hub_r = max(4, round(radius * 0.06))
        any_value = False
        for _, value, color in readings:
            if value is None:
                continue
            any_value = True
            frac = (value - min_value) / span
            angle = math.radians(frac_to_angle(frac))
            tip = (cx + needle_len * math.cos(angle), cy + needle_len * math.sin(angle))
            draw.line([(cx, cy), tip], fill=color, width=4)
        draw.ellipse([cx - hub_r, cy - hub_r, cx + hub_r, cy + hub_r], fill=COLOR_BLACK)

        if not any_value:
            draw.text((cx, cy + pad), "No Data", font=fonts["stat"], fill=COLOR_BLACK, anchor="ma")
            return

        if len(readings) == 1:
            _, value, _ = readings[0]
            draw.text((cx, cy + pad), f"{value:.{decimals}f} {unit}", font=fonts["stat"], fill=COLOR_BLACK, anchor="ma")
        else:
            # Multiple needles (e.g. split-phase Leg A/B voltage): a small
            # colored-swatch readout row instead of one big number, so each
            # leg's value and its needle color both stay unambiguous. Shrinks
            # to fit the gauge's width, and falls back to one reading per line
            # if it still doesn't fit even at the smallest usable size.
            texts = [f"{label}: {value:.{decimals}f}{unit}" if value is not None else f"{label}: --" for label, value, _ in readings]
            gap = 12
            available_w = (area_right - area_left) * 0.95

            def row_width(font, swatch):
                return sum(draw.textlength(t, font=font) + swatch + 8 for t in texts) + gap * (len(texts) - 1)

            read_font, swatch = fonts["stat"], fonts["small"].size
            width = row_width(read_font, swatch)
            if width > available_w:
                scale = max(0.55, available_w / width)
                read_font = get_font("Jost", max(9, round(read_font.size * scale)), "bold")
                swatch = read_font.size
                width = row_width(read_font, swatch)

            if width <= available_w:
                lx = cx - width / 2
                ly = cy + pad
                for (label, value, color), text in zip(readings, texts):
                    draw.rectangle([lx, ly + 4, lx + swatch, ly + 4 + swatch], fill=color, outline=COLOR_BLACK)
                    draw.text((lx + swatch + 6, ly), text, font=read_font, fill=COLOR_BLACK, anchor="la")
                    lx += draw.textlength(text, font=read_font) + swatch + 8 + gap
            else:
                # Still too wide even shrunk - stack one reading per line instead.
                line_h = read_font.size + 4
                ly = cy + pad
                for (label, value, color), text in zip(readings, texts):
                    text_w = draw.textlength(text, font=read_font)
                    lx = cx - (text_w + swatch + 6) / 2
                    draw.rectangle([lx, ly + 2, lx + swatch, ly + 2 + swatch], fill=color, outline=COLOR_BLACK)
                    draw.text((lx + swatch + 6, ly), text, font=read_font, fill=COLOR_BLACK, anchor="la")
                    ly += line_h
