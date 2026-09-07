from unittest.mock import AsyncMock

import pytest

from src.main import _ensure_live_location


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