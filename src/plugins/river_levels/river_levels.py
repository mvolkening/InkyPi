import base64
import io
import logging
import math
from datetime import datetime, timedelta, timezone

import pytz
import requests
from PIL import Image, ImageDraw, ImageEnhance, ImageOps

from plugins.base_plugin.base_plugin import BasePlugin

logger = logging.getLogger(__name__)

STATIONS_URL = "https://api.weather.gc.ca/collections/hydrometric-stations/items"
REALTIME_URL = "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# OSM's tile usage policy requires a descriptive User-Agent identifying the app.
# Refresh intervals should stay infrequent (e.g. hourly+) to stay within their
# acceptable-use guidelines: https://operations.osmfoundation.org/policies/tiles/
HEADERS = {"User-Agent": "InkyPi-RiverLevelsPlugin/1.0 (+https://github.com/fatihak/InkyPi)"}

REQUEST_TIMEOUT = 15
OVERPASS_TIMEOUT = 30

TILE_SIZE = 256
MIN_ZOOM = 2
MAX_ZOOM = 15
MAX_LAT = 85.05112878  # Web Mercator projection limit

MAX_STATION_POINTS = 10000  # comfortably covers a full 30-day / 5-min-interval window
CHART_TARGET_POINTS = 80

CHART_BOX_WIDTH = 150
CHART_BOX_HEIGHT = 74

MAX_WATERWAY_WAYS = 4000  # protects Overpass/render time for very large or dense areas
DEFAULT_RIVER_COLOR = "#0a3d67"

STATION_COLORS = [
    "#1f78b4", "#e31a1c", "#33a02c", "#ff7f00",
    "#6a3d9a", "#b15928", "#a6cee3", "#fb9a99",
]

# Waterway geometry for a given area never changes refresh-to-refresh, unlike the
# water level readings, so it's cached in-process (keyed by rounded bbox) to avoid
# re-querying the shared public Overpass API on every plugin refresh.
_WATERWAY_CACHE = {}


class RiverLevels(BasePlugin):
    def generate_settings_template(self):
        template_params = super().generate_settings_template()
        template_params['style_settings'] = True
        return template_params

    def generate_image(self, settings, device_config):
        bbox = self.parse_bbox(settings)
        days = self.parse_int(settings.get('days'), default=7, min_value=1, max_value=30)
        max_stations = self.parse_int(settings.get('maxStations'), default=8, min_value=1, max_value=20)
        metric_mode = settings.get('metricMode', 'auto')
        if metric_mode not in ('auto', 'level', 'discharge'):
            metric_mode = 'auto'
        title = (settings.get('customTitle') or '').strip() or 'River Levels'
        river_color = settings.get('riverColor') or DEFAULT_RIVER_COLOR

        dimensions = device_config.get_resolution()
        if device_config.get_config("orientation") == "vertical":
            dimensions = dimensions[::-1]

        try:
            stations = self.get_stations(bbox)
        except Exception as e:
            logger.error(f"Failed to retrieve hydrometric stations: {str(e)}")
            raise RuntimeError("Failed to retrieve hydrometric station list, please check logs.")

        if not stations:
            raise RuntimeError("No active, real-time hydrometric stations were found in the selected area. Try selecting a larger area on the map.")

        stations = self.select_stations(stations, max_stations)

        station_data = []
        for index, station in enumerate(stations):
            try:
                series = self.get_station_series(station['station_number'], days, metric_mode)
            except Exception as e:
                logger.warning(f"Failed to retrieve data for station {station['station_number']}: {str(e)}")
                continue
            if series is None:
                logger.info(f"No usable {metric_mode} data for station {station['station_number']}, skipping.")
                continue
            station_entry = {**station, **series, "color": STATION_COLORS[index % len(STATION_COLORS)]}
            station_data.append(station_entry)

        if not station_data:
            raise RuntimeError("Hydrometric data could not be retrieved for any station in the selected area, please check logs.")

        try:
            basemap, project = self.build_basemap(bbox, dimensions)
        except Exception as e:
            logger.error(f"Failed to build background map: {str(e)}")
            raise RuntimeError("Failed to retrieve background map tiles, please check logs.")

        waterways = self.get_waterways(bbox)
        if waterways:
            self.draw_waterways(basemap, waterways, project, dimensions, river_color)
        else:
            logger.info("No waterway geometry drawn (Overpass lookup returned nothing or failed); showing basemap only.")

        width, height = dimensions
        pixel_positions = [project(station['longitude'], station['latitude']) for station in station_data]
        boxes = self.layout_station_boxes(pixel_positions, dimensions)

        for station, (px, py), (box_left, box_top) in zip(station_data, pixel_positions, boxes):
            # positions are expressed as % of the container so they stay aligned
            # with the basemap image even though the outer plugin frame applies
            # its own padding/margins around our fixed-aspect-ratio canvas
            station['pin_left_pct'] = round(100 * px / width, 2)
            station['pin_top_pct'] = round(100 * py / height, 2)
            station['box_left_pct'] = round(100 * box_left / width, 2)
            station['box_top_pct'] = round(100 * box_top / height, 2)

        buffer = io.BytesIO()
        basemap.save(buffer, format="PNG")
        basemap_data_uri = "data:image/png;base64," + base64.b64encode(buffer.getvalue()).decode("ascii")

        timezone_name = device_config.get_config("timezone", default="America/New_York")
        time_format = device_config.get_config("time_format", default="12h")
        last_refresh_time = self.format_now(timezone_name, time_format)

        for station in station_data:
            station['latest_time_str'] = self.format_time(station['latest_time'], timezone_name, time_format)
            del station['latest_time']

        template_params = {
            "title": title,
            "basemap_data_uri": basemap_data_uri,
            "stations": station_data,
            "days": days,
            "last_refresh_time": last_refresh_time,
            "plugin_settings": settings,
        }

        image = self.render_image(dimensions, "river_levels.html", "river_levels.css", template_params)
        if not image:
            raise RuntimeError("Failed to take screenshot, please check logs.")
        return image

    def parse_bbox(self, settings):
        try:
            west = float(settings.get('west'))
            south = float(settings.get('south'))
            east = float(settings.get('east'))
            north = float(settings.get('north'))
        except (TypeError, ValueError):
            raise RuntimeError("Please select an area on the map.")

        if west >= east or south >= north:
            raise RuntimeError("Invalid map area selected, please try again.")

        return (west, south, east, north)

    def parse_int(self, value, default, min_value, max_value):
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = default
        return max(min_value, min(max_value, value))

    def get_stations(self, bbox):
        west, south, east, north = bbox
        params = {
            "bbox": f"{west},{south},{east},{north}",
            "STATUS_EN": "Active",
            "REAL_TIME": 1,
            "f": "json",
            "limit": 500,
        }
        response = requests.get(STATIONS_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"Hydrometric stations request failed with status {response.status_code}")

        stations = []
        for feature in response.json().get("features", []):
            props = feature.get("properties", {})
            coordinates = feature.get("geometry", {}).get("coordinates")
            station_number = props.get("STATION_NUMBER")
            if not coordinates or not station_number:
                continue
            stations.append({
                "station_number": station_number,
                "name": (props.get("STATION_NAME") or station_number).title(),
                "longitude": coordinates[0],
                "latitude": coordinates[1],
                "real_time": bool(props.get("REAL_TIME")),
                "drainage_area": props.get("DRAINAGE_AREA_GROSS") or 0,
            })
        return stations

    def select_stations(self, stations, max_stations):
        stations = sorted(stations, key=lambda s: (not s['real_time'], -(s['drainage_area'] or 0)))
        if len(stations) > max_stations:
            logger.info(f"Found {len(stations)} stations in the selected area, limiting to the {max_stations} largest/real-time stations.")
        return stations[:max_stations]

    def get_station_series(self, station_number, days, metric_mode):
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days)
        params = {
            "STATION_NUMBER": station_number,
            "datetime": f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            "sortby": "DATETIME",
            "f": "json",
            "limit": MAX_STATION_POINTS,
        }
        response = requests.get(REALTIME_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"Hydrometric realtime request failed with status {response.status_code}")

        readings = []
        for feature in response.json().get("features", []):
            props = feature.get("properties", {})
            raw_dt = props.get("DATETIME")
            if not raw_dt:
                continue
            dt = datetime.strptime(raw_dt, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            readings.append((dt, props.get("LEVEL"), props.get("DISCHARGE")))

        if not readings:
            return None

        discharge_count = sum(1 for _, _, discharge in readings if discharge is not None)
        level_count = sum(1 for _, level, _ in readings if level is not None)

        if metric_mode == "discharge":
            use_discharge = True
        elif metric_mode == "level":
            use_discharge = False
        else:
            use_discharge = discharge_count > 0 and discharge_count >= level_count * 0.5

        values = [(dt, discharge if use_discharge else level) for dt, level, discharge in readings]
        values = [(dt, value) for dt, value in values if value is not None]
        if not values:
            return None

        latest_time, latest_value = values[-1]
        trend = "steady"
        if len(values) >= 2:
            previous_value = values[-2][1]
            if latest_value > previous_value * 1.01:
                trend = "rising"
            elif latest_value < previous_value * 0.99:
                trend = "falling"

        chart_points = self.downsample(values, CHART_TARGET_POINTS)

        return {
            # plain ASCII avoids charset-detection mojibake risk in the headless
            # screenshot pipeline (the rendered HTML has no explicit <meta charset>)
            "unit": "m3/s" if use_discharge else "m",
            "metric_label": "Discharge" if use_discharge else "Level",
            "latest_value": round(latest_value, 2),
            "latest_time": latest_time,
            "trend": trend,
            "chart_values": [round(value, 2) for _, value in chart_points],
        }

    def downsample(self, values, target):
        if len(values) <= target:
            return values
        stride = len(values) / target
        result = [values[int(index * stride)] for index in range(target)]
        if result[-1] != values[-1]:
            result[-1] = values[-1]
        return result

    def lonlat_to_pixel(self, lon, lat, zoom):
        lat = max(min(lat, MAX_LAT), -MAX_LAT)
        n = 2 ** zoom
        x = (lon + 180.0) / 360.0 * n * TILE_SIZE
        lat_rad = math.radians(lat)
        y = (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n * TILE_SIZE
        return x, y

    def choose_zoom(self, bbox, dimensions):
        west, south, east, north = bbox
        for zoom in range(MAX_ZOOM, MIN_ZOOM - 1, -1):
            x1, y1 = self.lonlat_to_pixel(west, north, zoom)
            x2, y2 = self.lonlat_to_pixel(east, south, zoom)
            if abs(x2 - x1) <= dimensions[0] and abs(y2 - y1) <= dimensions[1]:
                return zoom
        return MIN_ZOOM

    def build_basemap(self, bbox, dimensions):
        west, south, east, north = bbox
        zoom = self.choose_zoom(bbox, dimensions)
        tile_count = 2 ** zoom

        x1, y1 = self.lonlat_to_pixel(west, north, zoom)
        x2, y2 = self.lonlat_to_pixel(east, south, zoom)
        center_x, center_y = (x1 + x2) / 2, (y1 + y2) / 2

        width, height = dimensions
        crop_left = center_x - width / 2
        crop_top = center_y - height / 2

        canvas = Image.new("RGB", dimensions, "#aad3df")

        tile_min_x = int(crop_left // TILE_SIZE)
        tile_max_x = int((crop_left + width) // TILE_SIZE)
        tile_min_y = int(crop_top // TILE_SIZE)
        tile_max_y = int((crop_top + height) // TILE_SIZE)

        for tile_x in range(tile_min_x, tile_max_x + 1):
            for tile_y in range(tile_min_y, tile_max_y + 1):
                if tile_x < 0 or tile_y < 0 or tile_x >= tile_count or tile_y >= tile_count:
                    continue
                tile_image = self.fetch_tile(zoom, tile_x, tile_y)
                if tile_image is None:
                    continue
                paste_x = int(tile_x * TILE_SIZE - crop_left)
                paste_y = int(tile_y * TILE_SIZE - crop_top)
                canvas.paste(tile_image, (paste_x, paste_y))

        def project(lon, lat):
            px, py = self.lonlat_to_pixel(lon, lat, zoom)
            return px - crop_left, py - crop_top

        # Standard OSM tile colours are subtle (pale creams/blues) meant for
        # full-colour LCDs. On a 6-7 colour e-ink panel they dither down to
        # near-white and water becomes indistinguishable from land, so the
        # basemap is muted to grayscale here and the actual river geometry is
        # drawn on top afterwards in one bold, deliberate colour instead.
        canvas = ImageOps.grayscale(canvas).convert("RGB")
        canvas = ImageEnhance.Contrast(canvas).enhance(1.35)

        return canvas, project

    def fetch_tile(self, zoom, x, y):
        url = OSM_TILE_URL.format(z=zoom, x=x, y=y)
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if not 200 <= response.status_code < 300:
                logger.warning(f"Failed to fetch map tile {url}: status {response.status_code}")
                return None
            return Image.open(io.BytesIO(response.content)).convert("RGB")
        except Exception as e:
            logger.warning(f"Failed to fetch map tile {url}: {str(e)}")
            return None

    def get_waterways(self, bbox):
        cache_key = tuple(round(value, 3) for value in bbox)
        if cache_key in _WATERWAY_CACHE:
            return _WATERWAY_CACHE[cache_key]

        west, south, east, north = bbox
        query = (
            "[out:json][timeout:25];"
            f'way["waterway"~"^(river|stream|canal)$"]["name"]({south},{west},{north},{east});'
            "out geom;"
        )
        try:
            response = requests.post(OVERPASS_URL, data={"data": query}, headers=HEADERS, timeout=OVERPASS_TIMEOUT)
            if not 200 <= response.status_code < 300:
                logger.warning(f"Overpass waterway request failed with status {response.status_code}")
                return []
            elements = response.json().get("elements", [])
        except Exception as e:
            logger.warning(f"Failed to retrieve waterway geometry from Overpass: {str(e)}")
            return []

        ways = []
        for element in elements[:MAX_WATERWAY_WAYS]:
            geometry = element.get("geometry")
            if not geometry:
                continue
            ways.append([(point["lon"], point["lat"]) for point in geometry])

        if len(elements) > MAX_WATERWAY_WAYS:
            logger.info(f"Overpass returned {len(elements)} waterway segments, drawing the first {MAX_WATERWAY_WAYS}.")

        _WATERWAY_CACHE[cache_key] = ways
        return ways

    def draw_waterways(self, canvas, waterways, project, dimensions, color):
        draw = ImageDraw.Draw(canvas)
        line_width = max(2, round(4 * dimensions[0] / 800))
        for way in waterways:
            points = [project(lon, lat) for lon, lat in way]
            if len(points) < 2:
                continue
            draw.line(points, fill=color, width=line_width, joint="curve")

    def layout_station_boxes(self, positions, dimensions):
        """Greedily place each station's chart box near its pin, picking whichever
        of 8 candidate anchor directions overlaps the least with boxes already
        placed for earlier (higher-priority) stations. Stations closer together
        than the box size will still overlap somewhat, but this keeps it to a
        minimum without needing a full constraint solver."""
        width, height = dimensions
        gap = 10
        offsets = [
            (gap, -CHART_BOX_HEIGHT / 2),                       # right
            (-CHART_BOX_WIDTH - gap, -CHART_BOX_HEIGHT / 2),    # left
            (-CHART_BOX_WIDTH / 2, -CHART_BOX_HEIGHT - gap),    # above
            (-CHART_BOX_WIDTH / 2, gap),                        # below
            (gap, -CHART_BOX_HEIGHT - gap),                     # top-right
            (gap, gap),                                         # bottom-right
            (-CHART_BOX_WIDTH - gap, -CHART_BOX_HEIGHT - gap),  # top-left
            (-CHART_BOX_WIDTH - gap, gap),                      # bottom-left
        ]

        def overlap_area(a, b):
            overlap_x = min(a[2], b[2]) - max(a[0], b[0])
            overlap_y = min(a[3], b[3]) - max(a[1], b[1])
            if overlap_x <= 0 or overlap_y <= 0:
                return 0
            return overlap_x * overlap_y

        placed_rects = []
        boxes = []
        for px, py in positions:
            best_rect = None
            best_score = None
            for dx, dy in offsets:
                left = min(max(px + dx, 0), max(width - CHART_BOX_WIDTH, 0))
                top = min(max(py + dy, 0), max(height - CHART_BOX_HEIGHT, 0))
                rect = (left, top, left + CHART_BOX_WIDTH, top + CHART_BOX_HEIGHT)
                score = sum(overlap_area(rect, other) for other in placed_rects)
                if best_score is None or score < best_score:
                    best_score, best_rect = score, rect
                if best_score == 0:
                    break
            placed_rects.append(best_rect)
            boxes.append((round(best_rect[0]), round(best_rect[1])))
        return boxes

    def format_time(self, dt, timezone_name, time_format):
        local_dt = dt.astimezone(pytz.timezone(timezone_name))
        if time_format == "24h":
            return local_dt.strftime("%H:%M")
        return local_dt.strftime("%-I:%M %p")

    def format_now(self, timezone_name, time_format):
        now = datetime.now(pytz.timezone(timezone_name))
        if time_format == "24h":
            return now.strftime("%Y-%m-%d %H:%M")
        return now.strftime("%Y-%m-%d %I:%M %p")
