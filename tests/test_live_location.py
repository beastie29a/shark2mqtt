from unittest.mock import AsyncMock

import pytest

from src.main import _ensure_live_location, _floor_file_updated_at


@pytest.mark.asyncio
async def test_ensure_live_location_enables_disabled_device():
    api = AsyncMock()
    device = type("Device", (), {
        "dsn": "SND1",
        "product_name": "Test Shark",
        "_properties": {"GET_live_location_switch": 0},
    })()
    configured = set()

    await _ensure_live_location(api, device, configured, enabled=True)

    api.set_desired_property.assert_awaited_once_with(
        "SND1", "live_location_switch", 1,
    )
    assert configured == set()


@pytest.mark.asyncio
async def test_ensure_live_location_does_not_rewrite_enabled_device():
    api = AsyncMock()
    device = type("Device", (), {
        "dsn": "SND1",
        "product_name": "Test Shark",
        "_properties": {"GET_live_location_switch": 1},
    })()
    configured = set()

    await _ensure_live_location(api, device, configured, enabled=True)

    api.set_desired_property.assert_not_awaited()
    assert configured == {"SND1"}


def test_floor_file_updated_at_reads_shadow_file_list():
    raw = {
        "shadow": {
            "properties": {
                "reported": {
                    "fileList": {
                        "Visual_Floor_1": {
                            "value": {"fileName": "floorRPfile1-1788841534.bin"},
                            "updatedAt": "2026-09-08T04:25:36.000Z",
                        }
                    }
                }
            }
        }
    }
    assert _floor_file_updated_at(raw) == "2026-09-08T04:25:36.000Z"


def test_floor_file_updated_at_missing_entries():
    assert _floor_file_updated_at({}) == ""
    assert _floor_file_updated_at({"shadow": {}}) == ""
    assert _floor_file_updated_at(
        {"shadow": {"properties": {"reported": {"fileList": {}}}}}
    ) == ""