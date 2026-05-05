"""Tests for mqtt_client module."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.mqtt_client import MqttClient


@pytest.mark.asyncio
async def test_mqtt_client_init():
    """Test MqttClient initialization."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_host = "localhost"
    mock_config.mqtt_port = 1883
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    assert mqtt_client._config == mock_config
    assert mqtt_client._prefix == "shark"
    assert mqtt_client._client is None


@pytest.mark.asyncio
async def test_mqtt_client_enter():
    """Test MqttClient async context manager enter."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_host = "localhost"
    mock_config.mqtt_port = 1883
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    with patch('aiomqtt.Client') as mock_client_class:
        mock_client_instance = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        async with mqtt_client as client:
            assert client is mqtt_client
            mock_client_class.assert_called_once_with(
                hostname="localhost",
                port=1883,
                username="user",
                password="pass",
                will=mock_client_instance.will
            )


@pytest.mark.asyncio
async def test_mqtt_client_exit():
    """Test MqttClient async context manager exit."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_host = "localhost"
    mock_config.mqtt_port = 1883
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    with patch('aiomqtt.Client') as mock_client_class:
        mock_client_instance = AsyncMock()
        mock_client_class.return_value.__aenter__.return_value = mock_client_instance

        async with mqtt_client:
            pass

        # Should have called __aexit__
        mock_client_instance.__aexit__.assert_called()


@pytest.mark.asyncio
async def test_mqtt_client_publish_discovery():
    """Test publish_discovery method."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    mock_device = MagicMock()
    mock_device.dsn = "device123"
    mock_device.product_name = "Shark Vacuum"
    mock_device.rooms = ["room1", "room2"]
    mock_device.room_name_map = {"room1": "Living Room"}
    mock_device.floor_id = "floor1"

    with patch.object(mqtt_client, '_client', AsyncMock()) as mock_client:
        await mqtt_client.publish_discovery(mock_device)

        # Should have called publish for discovery topics
        assert mock_client.publish.call_count >= 1


@pytest.mark.asyncio
async def test_mqtt_client_publish_state():
    """Test publish_state method."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    mock_device = MagicMock()
    mock_device.dsn = "device123"
    mock_device.product_name = "Shark Vacuum"
    mock_device.rooms = ["room1", "room2"]
    mock_device.room_name_map = {"room1": "Living Room"}
    mock_device.floor_id = "floor1"

    with patch.object(mqtt_client, '_client', AsyncMock()) as mock_client:
        await mqtt_client.publish_state(mock_device, prev_error=None)

        # Should have called publish for state topics
        assert mock_client.publish.call_count >= 1


@pytest.mark.asyncio
async def test_mqtt_client_publish_status():
    """Test publish_status method."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    with patch.object(mqtt_client, '_client', AsyncMock()) as mock_client:
        await mqtt_client.publish_status({"state": "online", "message": "running"})

        # Should have called publish for status topic
        mock_client.publish.assert_called_once()


@pytest.mark.asyncio
async def test_mqtt_client_publish_unavailable():
    """Test publish_unavailable method."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    mock_device1 = MagicMock()
    mock_device1.dsn = "device123"
    mock_device1.product_name = "Shark Vacuum"

    mock_device2 = MagicMock()
    mock_device2.dsn = "device456"
    mock_device2.product_name = "Shark Vacuum 2"

    with patch.object(mqtt_client, '_client', AsyncMock()) as mock_client:
        await mqtt_client.publish_unavailable([mock_device1, mock_device2])

        # Should have called publish for each device
        assert mock_client.publish.call_count == 2


@pytest.mark.asyncio
async def test_mqtt_client_command_handler():
    """Test command_handler method."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    # Mock the async context manager
    with patch.object(mqtt_client, '_client', AsyncMock()) as mock_client:
        # Mock the message handling
        mock_message = MagicMock()
        mock_message.topic = "shark/device123/command"
        mock_message.payload = b"clean"

        # Test that it doesn't crash
        await mqtt_client.command_handler(mock_message)

        # Should have subscribed to the topic
        mock_client.subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_mqtt_client_publish_discovery_with_no_rooms():
    """Test publish_discovery with no rooms."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    mock_device = MagicMock()
    mock_device.dsn = "device123"
    mock_device.product_name = "Shark Vacuum"
    mock_device.rooms = []
    mock_device.room_name_map = {}
    mock_device.floor_id = None

    with patch.object(mqtt_client, '_client', AsyncMock()) as mock_client:
        await mqtt_client.publish_discovery(mock_device)

        # Should have called publish for discovery topics
        assert mock_client.publish.call_count >= 1


@pytest.mark.asyncio
async def test_mqtt_client_publish_state_with_error():
    """Test publish_state with error handling."""
    mock_config = MagicMock()
    mock_config.mqtt_prefix = "shark"
    mock_config.mqtt_username = "user"
    mock_config.mqtt_password = "pass"

    mqtt_client = MqttClient(mock_config)

    mock_device = MagicMock()
    mock_device.dsn = "device123"
    mock_device.product_name = "Shark Vacuum"
    mock_device.rooms = ["room1"]
    mock_device.room_name_map = {"room1": "Living Room"}
    mock_device.floor_id = "floor1"

    with patch.object(mqtt_client, '_client', AsyncMock()) as mock_client:
        # Test with a payload that will cause an exception in publish
        with patch.object(mock_client, 'publish', side_effect=Exception("Publish failed")):
            await mqtt_client.publish_state(mock_device, prev_error=None)
            # Should not crash even with publish failure