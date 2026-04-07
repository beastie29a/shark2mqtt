import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from src.ayla_api import AylaApi

@pytest.mark.asyncio
async def test_ayla_api_init():
    """Test AylaAPI initialization"""
    api = AylaApi("test_user", "test_pass")
    assert api.username == "test_user"
    assert api.password == "test_pass"

@pytest.mark.asyncio
async def test_ayla_api_get_device_status():
    """Test get_device_status method"""
    with patch('src.ayla_api.AsyncClientSession', new_callable=AsyncMock) as mock_session:
        mock_session.return_value.__aenter__.return_value.json.return_value = {"status": "online"}
        
        api = AylaApi("test_user", "test_pass")
        result = await api.get_device_status("test_device")
        assert result == {"status": "online"}