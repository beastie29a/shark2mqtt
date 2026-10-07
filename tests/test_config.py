"""Tests for src.config.Settings map display options."""

from src.config import Settings


def _settings(**overrides) -> Settings:
    base = {
        "shark_username": "t@t.com",
        "shark_password": "x",
        "mqtt_host": "localhost",
    }
    base.update(overrides)
    return Settings(**base)


def test_map_show_defaults_all_on():
    """Defaults preserve the original rendering (all layers visible)."""
    s = _settings()
    assert s.map_show_background is True
    assert s.map_show_rooms is True
    assert s.map_show_obstacles is True
    assert s.map_show_robot is True


def test_map_show_overridable():
    s = _settings(
        map_show_background=False,
        map_show_rooms=False,
        map_show_obstacles=False,
        map_show_robot=False,
    )
    assert s.map_show_background is False
    assert s.map_show_rooms is False
    assert s.map_show_obstacles is False
    assert s.map_show_robot is False
