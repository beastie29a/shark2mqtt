from unittest.mock import AsyncMock, patch

import pytest

from src.shark_auth import SharkAuth


@pytest.mark.asyncio
async def test_shark_auth_init(mock_config):
    """Test SharkAuth initialization."""
    auth = SharkAuth(mock_config)
    config = auth.config
    assert config.username == mock_config.username
    assert config.password == mock_config.password

@pytest.mark.asyncio
async def test_shark_auth_authenticate(mock_config):
    """Test authentication method."""
    with patch('src.shark_auth.AsyncClientSession', new_callable=AsyncMock) as mock_session:
        mock_session.return_value.__aenter__.return_value.json.return_value = {"token": "test-token"}

        auth = SharkAuth(mock_config)
        result = await auth.authenticate()
        assert "token" in result
