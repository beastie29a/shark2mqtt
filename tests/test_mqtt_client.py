import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from src.mqtt_client import MqttClient

@pytest.mark.asyncio
async def test_mqtt_client_init():
    """Test MQTTClient initialization"""
    client = MqttClient("broker", 1883, "user", "pass")
    assert client.broker == "broker"
    assert client.port == 1883
    assert client.username == "user"
    assert client.password == "pass"

@pytest.mark.asyncio
async def test_mqtt_client_publish():
    """Test publish method"""
    with patch('src.mqtt_client.AsyncClient') as mock_client:
        mock_instance = mock_client.return_value
        mock_instance.publish = AsyncMock()
        
        client = MqttClient("broker", 1883, "user", "pass")
        await client.publish("topic", "message")
        mock_instance.publish.assert_called_once()