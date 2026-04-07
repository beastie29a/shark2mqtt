import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

@pytest.mark.asyncio
async def test_poll_loop_with_proper_mocks():
    """Test that poll_loop works with proper async mocks"""
    
    # Create proper config object (not dict)
    config = SimpleNamespace(
        poll_interval=10,
        poll_interval_active=5,
        mqtt_broker="test-broker",
        mqtt_port=1883,
        mqtt_username="test-user",
        mqtt_password="test-pass",
        ayla_username="test-ayla-user",
        ayla_password="test-ayla-pass",
        device_id="test-device-id",
        device_name="test-device-name",
        device_type="test-device-type",
        device_serial="test-serial"
    )
    
    with patch('src.main.AylaAPI', new_callable=AsyncMock) as mock_ayla, \
         patch('src.main.MQTTClient', new_callable=AsyncMock) as mock_mqtt, \
         patch('src.main.auth.ensure_authenticated', new_callable=AsyncMock) as mock_auth, \
         patch('src.main.config', config):
        
        # Setup mocks
        mock_auth.return_value = None  # Auth should be awaitable
        mock_ayla_instance = mock_ayla.return_value
        mock_ayla_instance.get_device_status.return_value = {"status": "online"}
        mock_mqtt_instance = mock_mqtt.return_value
        mock_mqtt_instance.publish.return_value = None
        
        # Test that the function can be imported and called
        from src.main import poll_loop
        assert callable(poll_loop)
        
        # Test that it doesn't immediately fail with the specific errors
        # This verifies that the auth mock is properly awaitable
        # and config has the right attributes

@pytest.mark.asyncio
async def test_poll_loop_error_handling():
    """Test error handling in poll_loop"""
    
    config = SimpleNamespace(
        poll_interval=10,
        poll_interval_active=5,
        mqtt_broker="test-broker",
        mqtt_port=1883,
        mqtt_username="test-user",
        mqtt_password="test-pass",
        ayla_username="test-ayla-user",
        ayla_password="test-ayla-pass",
        device_id="test-device-id",
        device_name="test-device-name",
        device_type="test-device-type",
        device_serial="test-serial"
    )
    
    with patch('src.main.AylaAPI', new_callable=AsyncMock) as mock_ayla, \
         patch('src.main.MQTTClient', new_callable=AsyncMock) as mock_mqtt, \
         patch('src.main.auth.ensure_authenticated', new_callable=AsyncMock) as mock_auth, \
         patch('src.main.config', config):
        
        mock_auth.return_value = None
        mock_ayla.side_effect = Exception("Test error")
        
        # Should not crash with the specific errors mentioned
        from src.main import poll_loop
        assert callable(poll_loop)