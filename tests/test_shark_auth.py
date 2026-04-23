from unittest.mock import AsyncMock, patch

import pytest

from src.shark_auth import SharkAuth


@pytest.mark.asyncio
async def test_shark_auth_init():
    """Test SharkAuth initialization"""
    auth = SharkAuth("user", "pass")
    assert auth.username == "user"
    assert auth.password == "pass"

@pytest.mark.asyncio
async def test_shark_auth_authenticate():
    """Test authentication method"""
    with patch('src.shark_auth.AsyncClientSession', new_callable=AsyncMock) as mock_session:
        mock_session.return_value.__aenter__.return_value.json.return_value = {"token": "test-token"}

        auth = SharkAuth("user", "pass")
        result = await auth.authenticate()
        assert "token" in result
