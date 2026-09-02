import sys
from pathlib import Path

# InkyPi runs with src/ on sys.path (src/inkypi.py), and plugin modules import
# via src-relative paths (e.g. `plugins.base_plugin...`). Mirror that here so
# tests can import plugin modules directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
