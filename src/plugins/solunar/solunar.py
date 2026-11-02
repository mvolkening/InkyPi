import logging
import math
import os
from datetime import date, datetime, timedelta, timezone

import pytz
import requests
from astral import LocationInfo
from astral.sun import sunrise as astral_sunrise, sunset as astral_sunset

from plugins.base_plugin.base_plugin import BasePlugin

logger = logging.getLogger(__name__)

OPEN_METEO_URL = (
    "https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={long}"
    "&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_sum,"
    "wind_direction_10m_dominant,wind_speed_10m_max"
    "&timezone=auto&forecast_days={forecast_days}"
)
OPEN_METEO_UNIT_PARAMS = {
    "metric": "temperature_unit=celsius&wind_speed_unit=kmh&precipitation_unit=mm",
    "imperial": "temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch",
}
UNIT_LABELS = {
    "metric": {"temperature": "°C", "speed": "km/h", "rain": "mm"},
    "imperial": {"temperature": "°F", "speed": "mph", "rain": "in"},
}
OPEN_METEO_MAX_DAYS = 16  # the free forecast endpoint's hard limit

DAYS_SHOWN = 29  # today + the next 28 days
LUNAR_CYCLE_DAYS = 29.530588853

MAJOR_HALF_WIDTH = timedelta(hours=1)    # majors: moon overhead / underfoot, ~2 h window
MINOR_HALF_WIDTH = timedelta(minutes=30)  # minors: moonrise / moonset, ~1 h window
SUN_OVERLAP_WINDOW = timedelta(hours=1)  # a period this close to sunrise/sunset boosts the rating
MOON_RISE_SET_ALTITUDE = 0.125  # degrees; accounts for parallax + refraction + semi-diameter

# Rating: 0-60 from moon phase (peak at new/full) + up to 40 for periods that line up with
# sunrise/sunset. A day scores "high" at or above HIGH_ACTIVITY_SCORE.
HIGH_ACTIVITY_SCORE = 60

WIND_COMPASS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
# Direction the wind blows FROM (meteorological), shown as an arrow pointing where it blows TO.
WIND_ARROWS = ["↓", "↙", "←", "↖", "↑", "↗", "→", "↘"]

DEFAULT_HIGH_COLOR = "#7fd37f"


def _rad(deg):
    return math.radians(deg)


def _deg(rad):
    return math.degrees(rad)


def _julian_day(dt_utc):
    return dt_utc.timestamp() / 86400.0 + 2440587.5


def _moon_sun_state(dt_utc):
    """Low-precision (~0.1-0.3 deg) geocentric Moon position plus the Sun's mean longitude.

    Based on the Schlyter orbital elements with the main perturbation terms. That is far
    more accuracy than solunar tables need (the Moon moves ~0.5 deg/hour).
    Returns (right_ascension_deg, declination_deg, elongation_deg).
    """
    d = _julian_day(dt_utc) - 2451543.5

    n_node = 125.1228 - 0.0529538083 * d
    incl = 5.1454
    w_moon = 318.0634 + 0.1643573223 * d
    a_moon = 60.2666
    e_moon = 0.054900
    m_moon = 115.3654 + 13.0649929509 * d

    w_sun = 282.9404 + 4.70935e-5 * d
    m_sun = 356.0470 + 0.9856002585 * d
    l_sun = m_sun + w_sun

    l_moon = m_moon + w_moon + n_node
    elong = (l_moon - l_sun) % 360.0
    f_arg = l_moon - n_node

    # Kepler's equation for the Moon's eccentric anomaly (iterated; e is not tiny)
    e_anom = m_moon + _deg(e_moon) * math.sin(_rad(m_moon)) * (1 + e_moon * math.cos(_rad(m_moon)))
    for _ in range(10):
        delta = (e_anom - _deg(e_moon) * math.sin(_rad(e_anom)) - m_moon) / (1 - e_moon * math.cos(_rad(e_anom)))
        e_anom -= delta
        if abs(delta) < 1e-6:
            break

    xv = a_moon * (math.cos(_rad(e_anom)) - e_moon)
    yv = a_moon * math.sqrt(1 - e_moon * e_moon) * math.sin(_rad(e_anom))
    v = _deg(math.atan2(yv, xv))
    r = math.hypot(xv, yv)

    vw = _rad(v + w_moon)
    xh = r * (math.cos(_rad(n_node)) * math.cos(vw) - math.sin(_rad(n_node)) * math.sin(vw) * math.cos(_rad(incl)))
    yh = r * (math.sin(_rad(n_node)) * math.cos(vw) + math.cos(_rad(n_node)) * math.sin(vw) * math.cos(_rad(incl)))
    zh = r * math.sin(vw) * math.sin(_rad(incl))

    lon = _deg(math.atan2(yh, xh))
    lat = _deg(math.atan2(zh, math.hypot(xh, yh)))

    m_m, m_s = m_moon, m_sun
    d_arg = elong
    lon += (-1.274 * math.sin(_rad(m_m - 2 * d_arg)) + 0.658 * math.sin(_rad(2 * d_arg))
            - 0.186 * math.sin(_rad(m_s)) - 0.059 * math.sin(_rad(2 * m_m - 2 * d_arg))
            - 0.057 * math.sin(_rad(m_m - 2 * d_arg + m_s)) + 0.053 * math.sin(_rad(m_m + 2 * d_arg))
            + 0.046 * math.sin(_rad(2 * d_arg - m_s)) + 0.041 * math.sin(_rad(m_m - m_s))
            - 0.035 * math.sin(_rad(d_arg)) - 0.031 * math.sin(_rad(m_m + m_s))
            - 0.015 * math.sin(_rad(2 * f_arg - 2 * d_arg)) + 0.011 * math.sin(_rad(m_m - 4 * d_arg)))
    lat += (-0.173 * math.sin(_rad(f_arg - 2 * d_arg)) - 0.055 * math.sin(_rad(m_m - f_arg - 2 * d_arg))
            - 0.046 * math.sin(_rad(m_m + f_arg - 2 * d_arg)) + 0.033 * math.sin(_rad(f_arg + 2 * d_arg))
            + 0.017 * math.sin(_rad(2 * m_m + f_arg)))

    # ecliptic -> equatorial
    ecl = _rad(23.4393 - 3.563e-7 * d)
    lon_r, lat_r = _rad(lon), _rad(lat)
    x = math.cos(lon_r) * math.cos(lat_r)
    y = math.sin(lon_r) * math.cos(lat_r) * math.cos(ecl) - math.sin(lat_r) * math.sin(ecl)
    z = math.sin(lon_r) * math.cos(lat_r) * math.sin(ecl) + math.sin(lat_r) * math.cos(ecl)
    ra = _deg(math.atan2(y, x)) % 360.0
    dec = _deg(math.asin(z))
    return ra, dec, elong


def _hour_angle(dt_utc, ra, longitude):
    """Moon hour angle in degrees, normalised to (-180, 180]. 0 = upper transit, 180 = lower."""
    jd = _julian_day(dt_utc)
    gmst = (280.46061837 + 360.98564736629 * (jd - 2451545.0)) % 360.0
    return (gmst + longitude - ra + 180.0) % 360.0 - 180.0


def _moon_altitude(dt_utc, latitude, longitude):
    ra, dec, _ = _moon_sun_state(dt_utc)
    ha = _rad(_hour_angle(dt_utc, ra, longitude))
    lat_r, dec_r = _rad(latitude), _rad(dec)
    return _deg(math.asin(math.sin(lat_r) * math.sin(dec_r) + math.cos(lat_r) * math.cos(dec_r) * math.cos(ha)))


def _bisect(func, lo, hi, iterations=30):
    """Find the zero crossing of func between datetimes lo and hi (func(lo), func(hi) differ in sign)."""
    f_lo = func(lo)
    for _ in range(iterations):
        mid = lo + (hi - lo) / 2
        f_mid = func(mid)
        if (f_mid > 0) == (f_lo > 0):
            lo, f_lo = mid, f_mid
        else:
            hi = mid
    return lo + (hi - lo) / 2


def find_moon_events(start_utc, end_utc, latitude, longitude, step_minutes=10):
    """Return sorted (datetime_utc, kind) tuples for kind in
    'upper' (moon overhead), 'lower' (moon underfoot), 'rise', 'set' between start and end."""
    events = []
    step = timedelta(minutes=step_minutes)

    def ha_upper(dt):
        ra, _, _ = _moon_sun_state(dt)
        return _hour_angle(dt, ra, longitude)

    def ha_lower(dt):
        ra, _, _ = _moon_sun_state(dt)
        return (_hour_angle(dt, ra, longitude) + 180.0 + 180.0) % 360.0 - 180.0

    def alt(dt):
        return _moon_altitude(dt, latitude, longitude) - MOON_RISE_SET_ALTITUDE

    t = start_utc
    prev_upper, prev_lower, prev_alt = ha_upper(t), ha_lower(t), alt(t)
    while t < end_utc:
        nxt = t + step
        cur_upper, cur_lower, cur_alt = ha_upper(nxt), ha_lower(nxt), alt(nxt)
        # hour angle increases through 0 at transit; ignore the +/-180 wrap-around jump
        if prev_upper < 0 <= cur_upper and cur_upper - prev_upper < 90:
            events.append((_bisect(ha_upper, t, nxt), "upper"))
        if prev_lower < 0 <= cur_lower and cur_lower - prev_lower < 90:
            events.append((_bisect(ha_lower, t, nxt), "lower"))
        if prev_alt < 0 <= cur_alt:
            events.append((_bisect(alt, t, nxt), "rise"))
        elif prev_alt >= 0 > cur_alt:
            events.append((_bisect(alt, t, nxt), "set"))
        t, prev_upper, prev_lower, prev_alt = nxt, cur_upper, cur_lower, cur_alt

    events.sort(key=lambda event: event[0])
    return events


def moon_phase_info(dt_utc):
    """Return (age_days, illumination_pct) for the moon at dt_utc."""
    _, _, elong = _moon_sun_state(dt_utc)
    age = elong / 360.0 * LUNAR_CYCLE_DAYS
    illum = (1 - math.cos(_rad(elong))) / 2 * 100
    return age, illum


def moon_phase_name(age):
    """Icon name (as used by the weather plugin's moon icons) for a moon age in days."""
    thresholds = [
        (1.0, "newmoon"),
        (6.4, "waxingcrescent"),
        (8.4, "firstquarter"),
        (13.8, "waxinggibbous"),
        (15.8, "fullmoon"),
        (21.2, "waninggibbous"),
        (23.2, "lastquarter"),
        (28.5, "waningcrescent"),
    ]
    for limit, name in thresholds:
        if age <= limit:
            return name
    return "newmoon"


def phase_score(age):
    """0-60 score: highest at new/full moon, fading to 0 about 4 days either side."""
    distance_to_new = min(age, LUNAR_CYCLE_DAYS - age)
    distance_to_full = abs(age - LUNAR_CYCLE_DAYS / 2)
    distance = min(distance_to_new, distance_to_full)
    if distance <= 1.5:
        return 60
    if distance <= 2.5:
        return 45
    if distance <= 3.5:
        return 20
    return 0


def wind_compass(deg):
    return WIND_COMPASS[int((deg % 360) / 45.0 + 0.5) % 8]


def wind_arrow(deg):
    return WIND_ARROWS[int((deg % 360) / 45.0 + 0.5) % 8]


class Solunar(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params['style_settings'] = True
        return template_params

    def generate_image(self, settings, device_config):
        raw_lat = (settings.get('latitude') or '').strip()
        raw_long = (settings.get('longitude') or '').strip()
        if not raw_lat or not raw_long:
            raise RuntimeError("Latitude and Longitude are required.")
        try:
            lat = float(raw_lat)
            long = float(raw_long)
        except (ValueError, TypeError):
            raise RuntimeError("Latitude and Longitude must be valid numbers.")

        units = settings.get('units', 'imperial')
        if units not in UNIT_LABELS:
            units = 'imperial'
        title = (settings.get('customTitle') or '').strip() or 'Solunar Calendar'
        time_format = device_config.get_config("time_format", default="12h")
        device_tz = pytz.timezone(device_config.get_config("timezone", default="America/New_York"))

        weather_by_date = {}
        tz = device_tz
        try:
            weather_data = self.get_weather_data(lat, long, units)
            weather_by_date = self.parse_weather(weather_data)
            if weather_data.get('timezone'):
                tz = pytz.timezone(weather_data['timezone'])
        except Exception as e:
            logger.error(f"Open-Meteo request failed, showing solunar data without weather: {e}")

        today = datetime.now(tz).date()
        days = self.build_days(today, tz, lat, long, weather_by_date, time_format)

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        # Calendar grid: columns are Sun..Sat, today's cell lands under its weekday.
        lead_blanks = (today.weekday() + 1) % 7
        cells = [None] * lead_blanks + days
        while len(cells) % 7:
            cells.append(None)
        weeks = [cells[i:i + 7] for i in range(0, len(cells), 7)]

        now = datetime.now(tz)
        last_refresh_time = now.strftime("%Y-%m-%d %H:%M") if time_format == "24h" else now.strftime("%Y-%m-%d %I:%M %p")

        template_params = {
            "title": title,
            "weeks": weeks,
            "weekday_labels": ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"],
            "labels": UNIT_LABELS[units],
            "icons_dir": os.path.join(os.path.dirname(self.get_plugin_dir()), "weather", "icons"),
            "high_color": settings.get('highColor') or DEFAULT_HIGH_COLOR,
            "last_refresh_time": last_refresh_time,
            "plugin_settings": settings,
        }

        image = self.render_image(dimensions, "solunar.html", "solunar.css", template_params)
        if not image:
            raise RuntimeError("Failed to take screenshot, please check logs.")
        return image

    def build_days(self, today, tz, lat, long, weather_by_date, time_format):
        # Scan a day either side so periods that spill across local midnight are still found
        # (the first/last day's neighbours also feed the sunrise/sunset overlap check).
        start_local = tz.localize(datetime.combine(today - timedelta(days=1), datetime.min.time()))
        end_local = tz.localize(datetime.combine(today + timedelta(days=DAYS_SHOWN + 1), datetime.min.time()))
        events = find_moon_events(start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc), lat, long)
        events = [(dt.astimezone(tz), kind) for dt, kind in events]

        observer = LocationInfo(latitude=lat, longitude=long).observer
        days = []
        for offset in range(DAYS_SHOWN):
            day = today + timedelta(days=offset)
            sunrise = self.sun_time(astral_sunrise, observer, day, tz)
            sunset = self.sun_time(astral_sunset, observer, day, tz)

            majors = [(dt, MAJOR_HALF_WIDTH) for dt, kind in events if dt.date() == day and kind in ("upper", "lower")]
            minors = [(dt, MINOR_HALF_WIDTH) for dt, kind in events if dt.date() == day and kind in ("rise", "set")]

            noon_utc = tz.localize(datetime.combine(day, datetime.min.time()).replace(hour=12)).astimezone(timezone.utc)
            age, illum = moon_phase_info(noon_utc)
            phase_name = moon_phase_name(age)

            score = phase_score(age) + self.sun_overlap_score(majors, minors, [sunrise, sunset])
            weather = weather_by_date.get(day)

            days.append({
                "date": day,
                "day_number": day.day,
                "month_label": day.strftime("%b") if (day.day == 1 or offset == 0) else "",
                "is_today": offset == 0,
                "sunrise": self.format_time(sunrise, time_format),
                "sunset": self.format_time(sunset, time_format),
                "major": self.split_am_pm(majors, time_format),
                "minor": self.split_am_pm(minors, time_format),
                "moon_icon": self.moon_icon_path(phase_name, lat),
                "moon_pct": f"{illum:.0f}",
                "score": score,
                "high": score >= HIGH_ACTIVITY_SCORE,
                "weather": weather,
            })
        return days

    @staticmethod
    def sun_time(func, observer, day, tz):
        try:
            return func(observer, date=day, tzinfo=tz)
        except ValueError:
            return None  # polar day/night: sun doesn't rise or set

    @staticmethod
    def sun_overlap_score(majors, minors, sun_events):
        """+20 per major / +10 per minor falling within an hour of sunrise or sunset (max 40)."""
        score = 0
        for dt, _ in majors:
            if any(s and abs(dt - s) <= SUN_OVERLAP_WINDOW for s in sun_events):
                score += 20
        for dt, _ in minors:
            if any(s and abs(dt - s) <= SUN_OVERLAP_WINDOW for s in sun_events):
                score += 10
        return min(score, 40)

    @staticmethod
    def format_time(dt, time_format):
        if dt is None:
            return "--"
        if time_format == "24h":
            return dt.strftime("%H:%M")
        return dt.strftime("%I:%M").lstrip("0") + dt.strftime("%p")[0].lower()

    def split_am_pm(self, periods, time_format):
        """Place periods into morning (before noon) and afternoon slots; None-safe when one is missing."""
        slots = {"am": "--", "pm": "--"}
        for dt, _ in sorted(periods):
            slot = "am" if dt.hour < 12 else "pm"
            if slots[slot] == "--":
                slots[slot] = self.format_time(dt, time_format)
        return slots

    def moon_icon_path(self, phase_name, lat):
        # The waxing/waning sides swap between hemispheres, as in the weather plugin.
        if lat < 0:
            phase_name = {
                "waxingcrescent": "waningcrescent", "waningcrescent": "waxingcrescent",
                "waxinggibbous": "waninggibbous", "waninggibbous": "waxinggibbous",
                "firstquarter": "lastquarter", "lastquarter": "firstquarter",
            }.get(phase_name, phase_name)
        plugins_dir = os.path.dirname(self.get_plugin_dir())
        return os.path.join(plugins_dir, "weather", "icons", f"{phase_name}.png")

    def weather_icon_path(self, code):
        plugins_dir = os.path.dirname(self.get_plugin_dir())
        icon = {0: "01d", 1: "022d", 2: "02d", 3: "04d", 45: "50d", 48: "48d",
                51: "51d", 61: "51d", 80: "51d", 53: "53d", 63: "53d", 81: "53d",
                55: "09d", 65: "09d", 82: "09d", 56: "56d", 66: "56d", 57: "57d", 67: "57d",
                71: "71d", 85: "71d", 73: "73d", 75: "13d", 86: "13d", 77: "77d",
                95: "11d", 96: "11d", 99: "11d"}.get(code, "01d")
        return os.path.join(plugins_dir, "weather", "icons", f"{icon}.png")

    def parse_weather(self, weather_data):
        daily = weather_data.get('daily', {})
        by_date = {}
        for i, day_str in enumerate(daily.get('time', [])):
            def pick(key, default=None):
                values = daily.get(key, [])
                return values[i] if i < len(values) and values[i] is not None else default

            wind_deg = pick('wind_direction_10m_dominant')
            rain = pick('precipitation_sum', 0)
            by_date[date.fromisoformat(day_str)] = {
                "icon": self.weather_icon_path(pick('weather_code', 0)),
                "high": round(pick('temperature_2m_max', 0)),
                "low": round(pick('temperature_2m_min', 0)),
                "wind_dir": wind_compass(wind_deg) if wind_deg is not None else "",
                "wind_arrow": wind_arrow(wind_deg) if wind_deg is not None else "",
                "wind_speed": round(pick('wind_speed_10m_max', 0)),
                "rain": f"{rain:.2f}" if weather_data.get('daily_units', {}).get('precipitation_sum') == 'inch' else f"{rain:.1f}",
            }
        return by_date

    def get_weather_data(self, lat, long, units):
        url = OPEN_METEO_URL.format(lat=lat, long=long, forecast_days=OPEN_METEO_MAX_DAYS)
        url += f"&{OPEN_METEO_UNIT_PARAMS[units]}"
        response = requests.get(url, timeout=30)
        if not 200 <= response.status_code < 300:
            logger.error(f"Failed to retrieve Open-Meteo weather data: {response.content}")
            raise RuntimeError("Failed to retrieve Open-Meteo weather data.")
        return response.json()
