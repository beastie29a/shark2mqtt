"""Persistent per-device map rendering preferences."""

from __future__ import annotations

import json
import logging
import tempfile
from collections.abc import Mapping
from pathlib import Path

logger = logging.getLogger(__name__)

MAP_PREFERENCE_OPTIONS: dict[str, tuple[str, str, str]] = {
    "background": ("map_show_background", "Show Background", "mdi:image-outline"),
    "rooms": ("map_show_rooms", "Show Rooms", "mdi:floor-plan"),
    "obstacles": ("map_show_obstacles", "Show Obstacles", "mdi:wall"),
    "robot": ("map_show_robot", "Show Robot", "mdi:robot-vacuum"),
}


class MapPreferenceStore:
    """Store map visibility overrides by device DSN."""

    def __init__(self, path: Path, defaults: Mapping[str, bool]) -> None:
        self._path = path
        self._defaults = {
            key: defaults.get(key, True) for key in MAP_PREFERENCE_OPTIONS
        }
        self._overrides = self._load()

    def get(self, dsn: str) -> dict[str, bool]:
        """Return effective map visibility settings for a device."""
        return {
            key: self._overrides.get(dsn, {}).get(key, default)
            for key, default in self._defaults.items()
        }

    def set(self, dsn: str, option: str, value: bool) -> bool:
        """Persist an override and return whether its effective value changed."""
        if option not in MAP_PREFERENCE_OPTIONS:
            raise ValueError(f"Unknown map preference: {option}")

        changed = self.get(dsn)[option] != value
        self._overrides.setdefault(dsn, {})[option] = value
        self._save()
        return changed

    def _load(self) -> dict[str, dict[str, bool]]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as err:
            logger.warning("Failed to load map preferences: %s", err)
            return {}

        devices = data.get("devices") if isinstance(data, dict) else None
        if not isinstance(devices, dict):
            logger.warning("Map preference file has an invalid format")
            return {}

        overrides: dict[str, dict[str, bool]] = {}
        for dsn, values in devices.items():
            if not isinstance(dsn, str) or not isinstance(values, dict):
                continue
            valid_values = {
                key: value
                for key, value in values.items()
                if key in MAP_PREFERENCE_OPTIONS and type(value) is bool
            }
            if valid_values:
                overrides[dsn] = valid_values
        return overrides

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self._path.parent,
                suffix=".tmp",
                delete=False,
            ) as file:
                temporary_path = Path(file.name)
                json.dump({"devices": self._overrides}, file, indent=2, sort_keys=True)
                file.write("\n")
            temporary_path.replace(self._path)
        except Exception:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
            raise
