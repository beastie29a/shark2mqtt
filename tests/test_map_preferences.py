"""Tests for per-device map visibility preferences."""

import json

import pytest
from src.map_preferences import MAP_PREFERENCE_OPTIONS, MapPreferenceStore

DEFAULTS = {key: True for key in MAP_PREFERENCE_OPTIONS}


def test_missing_file_uses_defaults(tmp_path):
    store = MapPreferenceStore(tmp_path / "map_preferences.json", DEFAULTS)

    assert store.get("DSN1") == DEFAULTS


def test_preferences_are_isolated_and_persisted(tmp_path):
    path = tmp_path / "data" / "map_preferences.json"
    store = MapPreferenceStore(path, DEFAULTS)

    assert store.set("DSN1", "background", False)
    assert store.get("DSN1")["background"] is False
    assert store.get("DSN2")["background"] is True

    reloaded = MapPreferenceStore(path, DEFAULTS)
    assert reloaded.get("DSN1")["background"] is False
    assert reloaded.get("DSN2") == DEFAULTS


def test_overrides_fall_back_to_global_defaults(tmp_path):
    defaults = {**DEFAULTS, "rooms": False}
    path = tmp_path / "map_preferences.json"
    path.write_text(json.dumps({"devices": {"DSN1": {"background": False}}}))

    store = MapPreferenceStore(path, defaults)

    assert store.get("DSN1") == {**defaults, "background": False}


@pytest.mark.parametrize("contents", ["{invalid", "[]", '{"devices": []}'])
def test_invalid_file_uses_defaults(tmp_path, contents):
    path = tmp_path / "map_preferences.json"
    path.write_text(contents)

    store = MapPreferenceStore(path, DEFAULTS)

    assert store.get("DSN1") == DEFAULTS


def test_unknown_preferences_are_ignored(tmp_path):
    path = tmp_path / "map_preferences.json"
    path.write_text(
        json.dumps({"devices": {"DSN1": {"background": "false", "unknown": False}}})
    )

    store = MapPreferenceStore(path, DEFAULTS)

    assert store.get("DSN1") == DEFAULTS


def test_setting_same_effective_value_reports_unchanged(tmp_path):
    store = MapPreferenceStore(tmp_path / "map_preferences.json", DEFAULTS)

    assert not store.set("DSN1", "background", True)
    assert store.get("DSN1")["background"] is True
