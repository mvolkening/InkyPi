import base64
import csv
import io
import logging
import os
import random
import re
from datetime import datetime

import pytz
import requests

from plugins.base_plugin.base_plugin import BasePlugin

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 15

# The sheet used to design/validate this plugin - a handy default so the
# plugin renders something meaningful before a user swaps in their own sheet.
DEFAULT_SHEET_URL = "https://docs.google.com/spreadsheets/d/1-xHxi7RQPxeqjaxZOak_wkbYVYrUvuJD5lWuBgAksCw/edit?usp=sharing"

POINTS_BY_PLACE = {"winner": 3, "second": 2, "third": 1}
REQUIRED_COLUMNS = {"winner", "second", "third"}

THEMES = ("medieval", "scifi", "fairytale")
DEFAULT_THEME = "medieval"
THEME_MODES = ("fixed", "random")

# Rank decorations shown for 1st/2nd/3rd place, styled to match each theme.
RANK_DECORATIONS = {
    "medieval": {1: "\U0001F451", 2: "\U0001F6E1️", 3: "\U00002694️"},   # crown, shield, crossed swords
    "scifi": {1: "\U0001F947", 2: "\U0001F948", 3: "\U0001F949"},                  # gold/silver/bronze medal
    "fairytale": {1: "\U00002B50", 2: "\U00002728", 3: "\U0001F31F"},             # star, sparkles, glowing star
}

# Bundled default background art for each theme - each has the same blank
# center panel (scroll / gate / hologram screen) laid out at roughly the same
# spot, which is where the .scoreboard-panel is positioned in scoreboard.css.
THEME_BACKGROUND_FILES = {
    "medieval": "medieval_background.png",
    "scifi": "scifi_background.png",
    "fairytale": "fary_tale_background.png",
}

# Bundled theme backgrounds never change at runtime, so the (fairly large)
# base64 encoding only needs to happen once per theme, not on every refresh.
_BACKGROUND_DATA_URI_CACHE = {}

SHEET_ID_PATTERN = re.compile(r"/d/([a-zA-Z0-9_-]+)")
SHEET_ID_ONLY_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


class Scoreboard(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params['style_settings'] = True
        return template_params

    def generate_image(self, settings, device_config):
        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        sheet_url = (settings.get('sheetUrl') or '').strip() or DEFAULT_SHEET_URL

        theme_mode = settings.get('themeMode')
        if theme_mode not in THEME_MODES:
            theme_mode = 'fixed'
        if theme_mode == 'random':
            theme = random.choice(THEMES)
        else:
            theme = settings.get('theme')
            if theme not in THEMES:
                theme = DEFAULT_THEME

        title = (settings.get('title') or '').strip() or 'Leaderboard'
        game_filter = (settings.get('gameFilter') or '').strip()
        max_players = self.parse_int(settings.get('maxPlayers'), default=5, min_value=3, max_value=50)
        outline_width = self.parse_int(settings.get('outlineWidth'), default=5, min_value=0, max_value=15)
        outline_color = settings.get('outlineColor') or '#ffffff'

        background_data_uri = self.get_background_data_uri(settings, theme)

        csv_url = self.build_csv_url(sheet_url)
        rows, fieldnames = self.fetch_rows(csv_url)
        self.validate_columns(fieldnames)

        leaderboard, games_counted = self.build_leaderboard(rows, game_filter)
        leaderboard = leaderboard[:max_players]

        decorations = RANK_DECORATIONS.get(theme, {})
        for index, entry in enumerate(leaderboard, start=1):
            entry['rank'] = index
            entry['medal'] = decorations.get(index)

        timezone_name = device_config.get_config("timezone", default="America/New_York")
        time_format = device_config.get_config("time_format", default="12h")

        template_params = {
            "title": title,
            "theme": theme,
            "background_data_uri": background_data_uri,
            "outline_width": outline_width,
            "outline_color": outline_color,
            "leaderboard": leaderboard,
            "game_filter": game_filter,
            "games_counted": games_counted,
            "last_refresh_time": self.format_now(timezone_name, time_format),
            "plugin_settings": settings,
        }

        return self.render_image(dimensions, "scoreboard.html", "scoreboard.css", template_params)

    def get_background_data_uri(self, settings, theme):
        # A user-uploaded background (via the standard Style > Background >
        # Image option) overrides the theme's bundled default artwork.
        if settings.get('backgroundOption') == 'image' and settings.get('backgroundImageFile'):
            return self.load_image_as_data_uri(settings['backgroundImageFile'])

        filename = THEME_BACKGROUND_FILES.get(theme)
        if filename not in _BACKGROUND_DATA_URI_CACHE:
            _BACKGROUND_DATA_URI_CACHE[filename] = self.load_image_as_data_uri(self.get_plugin_dir(filename))
        return _BACKGROUND_DATA_URI_CACHE[filename]

    def load_image_as_data_uri(self, path):
        try:
            with open(path, 'rb') as image_file:
                data = image_file.read()
        except Exception as e:
            logger.warning(f"Failed to load background image {path}: {str(e)}")
            return None

        extension = os.path.splitext(path)[1].lstrip('.').lower() or 'png'
        mime_subtype = 'jpeg' if extension in ('jpg', 'jpeg') else extension
        return f"data:image/{mime_subtype};base64," + base64.b64encode(data).decode('ascii')

    def parse_int(self, value, default, min_value, max_value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = default
        return max(min_value, min(max_value, value))

    def build_csv_url(self, sheet_url):
        match = SHEET_ID_PATTERN.search(sheet_url)
        if match:
            sheet_id = match.group(1)
        elif SHEET_ID_ONLY_PATTERN.match(sheet_url):
            # allow pasting the bare sheet ID instead of the full URL
            sheet_id = sheet_url
        else:
            raise RuntimeError("Could not find a Google Sheet ID in the provided URL. Please paste the full 'Share' link to your Google Sheet.")
        return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

    def fetch_rows(self, csv_url):
        try:
            response = requests.get(csv_url, timeout=REQUEST_TIMEOUT)
        except Exception as e:
            logger.error(f"Failed to fetch scoreboard sheet: {str(e)}")
            raise RuntimeError("Failed to reach Google Sheets, please check logs.")

        if not 200 <= response.status_code < 300:
            raise RuntimeError(
                f"Google Sheets request failed with status {response.status_code}. "
                "Make sure the sheet's sharing setting is \"Anyone with the link can view\"."
            )

        # Google's CSV export doesn't send a charset in the Content-Type header,
        # so requests falls back to guessing (often Latin-1) and mangles any
        # non-ASCII names (e.g. "Bärbel" -> "BÃ¤rbel"). The export is always UTF-8.
        text = response.content.decode('utf-8')
        if text.lstrip()[:15].lower().startswith(("<!doctype", "<html")):
            raise RuntimeError(
                "Google Sheets did not return spreadsheet data. Make sure the sheet's sharing "
                "setting is \"Anyone with the link can view\" and the link points to a real sheet."
            )

        reader = csv.DictReader(io.StringIO(text))
        fieldnames = [(name or '').strip().lower() for name in (reader.fieldnames or [])]

        rows = []
        for raw_row in reader:
            row = {}
            for key, value in raw_row.items():
                if key is None:
                    continue
                row[key.strip().lower()] = (value or '').strip()
            rows.append(row)
        return rows, fieldnames

    def validate_columns(self, fieldnames):
        missing = REQUIRED_COLUMNS - set(fieldnames)
        if missing:
            raise RuntimeError(
                f"Sheet is missing expected column(s): {', '.join(sorted(missing))}. "
                "See the Scoreboard plugin's README for the expected Google Form/Sheet layout."
            )

    def build_leaderboard(self, rows, game_filter):
        stats = {}

        def add_points(name, points, place_key):
            name = name.strip()
            if not name:
                return
            entry = stats.setdefault(name, {"name": name, "points": 0, "wins": 0, "seconds": 0, "thirds": 0})
            entry["points"] += points
            entry[place_key] += 1

        games_counted = 0
        for row in rows:
            if game_filter and row.get("game", "").strip().lower() != game_filter.lower():
                continue

            winner, second, third = row.get("winner", ""), row.get("second", ""), row.get("third", "")
            if not (winner or second or third):
                continue

            games_counted += 1
            add_points(winner, POINTS_BY_PLACE["winner"], "wins")
            add_points(second, POINTS_BY_PLACE["second"], "seconds")
            add_points(third, POINTS_BY_PLACE["third"], "thirds")

        leaderboard = sorted(
            stats.values(),
            key=lambda entry: (-entry["points"], -entry["wins"], -entry["seconds"], entry["name"].lower())
        )
        return leaderboard, games_counted

    def format_now(self, timezone_name, time_format):
        now = datetime.now(pytz.timezone(timezone_name))
        if time_format == "24h":
            return now.strftime("%Y-%m-%d %H:%M")
        return now.strftime("%Y-%m-%d %I:%M %p")
