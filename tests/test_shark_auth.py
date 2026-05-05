"""Tests for shark_auth module."""

import asyncio
import json
import os
import tempfile
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.shark_auth import SharkAuth, TokenData
from src.exc import SharkAuthError, SharkAuthLockedError


@pytest.mark.asyncio
async def test_shark_auth_init():
    """Test SharkAuth initialization."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    assert auth.config == mock_config
    assert auth._tokens is None
    assert auth._consecutive_failures == 0
    assert auth._backoff_until == 0
    assert auth._browser_launches_today == 0
    assert auth._browser_launch_day == 0


@pytest.mark.asyncio
async def test_shark_auth_id_token_property():
    """Test id_token property."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Test when no tokens
    assert auth.id_token is None

    # Test when tokens exist
    mock_tokens = MagicMock()
    mock_tokens.auth0_id_token = "test_token"
    auth._tokens = mock_tokens

    assert auth.id_token == "test_token"


@pytest.mark.asyncio
async def test_shark_auth_ayla_access_token_property():
    """Test ayla_access_token property."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Test when no tokens
    assert auth.ayla_access_token is None

    # Test when tokens exist
    mock_tokens = MagicMock()
    mock_tokens.ayla_access_token = "test_ayla_token"
    auth._tokens = mock_tokens

    assert auth.ayla_access_token == "test_ayla_token"


@pytest.mark.asyncio
async def test_shark_auth_ayla_refresh_token_property():
    """Test ayla_refresh_token property."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Test when no tokens
    assert auth.ayla_refresh_token is None

    # Test when tokens exist
    mock_tokens = MagicMock()
    mock_tokens.ayla_refresh_token = "test_refresh_token"
    auth._tokens = mock_tokens

    assert auth.ayla_refresh_token == "test_refresh_token"


@pytest.mark.asyncio
async def test_shark_auth_update_ayla_tokens():
    """Test update_ayla_tokens method."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    with patch.object(auth, '_save_tokens') as mock_save:
        auth.update_ayla_tokens("access123", "refresh456", datetime.now())

        assert auth._tokens.ayla_access_token == "access123"
        assert auth._tokens.ayla_refresh_token == "refresh456"
        mock_save.assert_called_once()


@pytest.mark.asyncio
async def test_shark_auth_ensure_authenticated_with_cached_token():
    """Test ensure_authenticated with cached token."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Set up cached tokens
    mock_tokens = MagicMock()
    mock_tokens.auth0_id_token = "cached_token"
    auth._tokens = mock_tokens

    with patch.object(auth, '_load_tokens', return_value=mock_tokens):
        token = await auth.ensure_authenticated()

        assert token == "cached_token"


@pytest.mark.asyncio
async def test_shark_auth_ensure_authenticated_refresh_token_grant():
    """Test ensure_authenticated with refresh token grant."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Set up tokens with refresh token
    mock_tokens = MagicMock()
    mock_tokens.auth0_refresh_token = "refresh_token"
    mock_tokens.auth0_id_token = None
    auth._tokens = mock_tokens

    with patch.object(auth, '_load_tokens', return_value=mock_tokens):
        with patch.object(auth, '_refresh_auth0_token', return_value=None) as mock_refresh:
            with patch.object(auth, '_browser_authenticate', side_effect=SharkAuthError("No browser auth")):
                token = await auth.ensure_authenticated()

                # Should have refreshed token
                mock_refresh.assert_called_once()


@pytest.mark.asyncio
async def test_shark_auth_ensure_authenticated_browser_auth():
    """Test ensure_authenticated with browser auth."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Set up tokens without refresh token
    mock_tokens = MagicMock()
    mock_tokens.auth0_refresh_token = None
    mock_tokens.auth0_id_token = None
    auth._tokens = mock_tokens

    with patch.object(auth, '_load_tokens', return_value=mock_tokens):
        with patch.object(auth, '_refresh_auth0_token', side_effect=SharkAuthError("No refresh")):
            with patch.object(auth, '_browser_authenticate', return_value=None) as mock_browser:
                token = await auth.ensure_authenticated()

                # Should have attempted browser auth
                mock_browser.assert_called_once()


@pytest.mark.asyncio
async def test_shark_auth_ensure_authenticated_circuit_breaker():
    """Test ensure_authenticated with circuit breaker active."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Set up circuit breaker
    auth._backoff_until = 1000000000  # Far in the future

    with pytest.raises(SharkAuthError):
        await auth.ensure_authenticated()


@pytest.mark.asyncio
async def test_shark_auth_refresh_auth0_token_success():
    """Test _refresh_auth0_token with success."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Set up tokens
    mock_tokens = MagicMock()
    mock_tokens.auth0_refresh_token = "refresh_token"
    mock_tokens.auth0_id_token = "old_token"
    auth._tokens = mock_tokens

    # Mock aiohttp response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "id_token": "new_token",
        "access_token": "access_token",
        "refresh_token": "new_refresh_token"
    })

    with patch('aiohttp.ClientSession') as mock_session:
        mock_session_instance = AsyncMock()
        mock_session_instance.post = AsyncMock(return_value=mock_response)
        mock_session.return_value.__aenter__.return_value = mock_session_instance

        await auth._refresh_auth0_token()

        assert auth._tokens.auth0_id_token == "new_token"
        assert auth._tokens.auth0_access_token == "access_token"
        assert auth._tokens.auth0_refresh_token == "new_refresh_token"


@pytest.mark.asyncio
async def test_shark_auth_refresh_auth0_token_failure():
    """Test _refresh_auth0_token with failure."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # No tokens
    with pytest.raises(SharkAuthError):
        await auth._refresh_auth0_token()


@pytest.mark.asyncio
async def test_shark_auth_load_tokens_success():
    """Test _load_tokens with success."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Create a mock token file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        token_data = {
            "auth0_refresh_token": "refresh_token",
            "auth0_id_token": "id_token",
            "auth0_access_token": "access_token",
            "ayla_access_token": "ayla_access_token",
            "ayla_refresh_token": "ayla_refresh_token",
            "ayla_token_expiry": "2023-01-01T00:00:00Z",
            "saved_at": "2023-01-01T00:00:00Z"
        }
        f.write(json.dumps(token_data))
        token_file = f.name

    # Set the token path to our temp file
    auth._token_path = token_file

    try:
        tokens = auth._load_tokens()
        assert tokens.auth0_refresh_token == "refresh_token"
        assert tokens.auth0_id_token == "id_token"
    finally:
        os.unlink(token_file)


@pytest.mark.asyncio
async def test_shark_auth_load_tokens_file_not_exists():
    """Test _load_tokens when file doesn't exist."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Set token path to non-existent file
    auth._token_path = "/non/existent/file.json"

    tokens = auth._load_tokens()
    assert tokens is None


@pytest.mark.asyncio
async def test_shark_auth_save_tokens_success():
    """Test _save_tokens with success."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Create a mock token
    auth._tokens = TokenData(
        auth0_refresh_token="refresh_token",
        auth0_id_token="id_token",
        auth0_access_token="access_token",
        ayla_access_token="ayla_access_token",
        ayla_refresh_token="ayla_refresh_token",
        ayla_token_expiry="2023-01-01T00:00:00Z",
        saved_at="2023-01-01T00:00:00Z"
    )

    with patch('tempfile.mkstemp') as mock_mkstemp:
        mock_mkstemp.return_value = (1, "/tmp/temp_file")
        with patch('os.fdopen') as mock_fdopen:
            with patch('os.replace') as mock_replace:
                mock_fdopen.return_value.__enter__.return_value.write = MagicMock()
                mock_replace.return_value = None

                auth._save_tokens()

                # Should have been called
                mock_fdopen.assert_called_once()


@pytest.mark.asyncio
async def test_shark_auth_check_browser_rate_limit():
    """Test _check_browser_rate_limit."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Test normal case (under limit)
    auth._browser_launches_today = 2
    auth._browser_launch_day = datetime.now().timetuple().tm_yday

    # Should not raise
    auth._check_browser_rate_limit()


@pytest.mark.asyncio
async def test_shark_auth_check_browser_rate_limit_exceeded():
    """Test _check_browser_rate_limit when exceeded."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Set up exceeding limit
    auth._browser_launches_today = 3  # MAX_BROWSER_LAUNCHES_PER_DAY = 3
    auth._browser_launch_day = datetime.now().timetuple().tm_yday

    with pytest.raises(SharkAuthLockedError):
        auth._check_browser_rate_limit()


@pytest.mark.asyncio
async def test_shark_auth_record_browser_launch():
    """Test _record_browser_launch."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Test recording a launch
    auth._record_browser_launch()

    # Should have incremented
    assert auth._browser_launches_today == 1


@pytest.mark.asyncio
async def test_shark_auth_generate_pkce_pair():
    """Test generate_pkce_pair."""
    verifier, challenge = SharkAuth.generate_pkce_pair()

    assert isinstance(verifier, str)
    assert isinstance(challenge, str)
    assert len(verifier) > 0
    assert len(challenge) > 0


@pytest.mark.asyncio
async def test_shark_auth_exchange_code_for_tokens_success():
    """Test exchange_code_for_tokens with success."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Mock aiohttp response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={
        "id_token": "new_id_token",
        "access_token": "access_token",
        "refresh_token": "new_refresh_token"
    })

    with patch('aiohttp.ClientSession') as mock_session:
        mock_session_instance = AsyncMock()
        mock_session_instance.post = AsyncMock(return_value=mock_response)
        mock_session.return_value.__aenter__.return_value = mock_session_instance

        await auth.exchange_code_for_tokens("auth_code", "verifier")

        # Should have updated tokens
        assert auth._tokens.auth0_id_token == "new_id_token"
        assert auth._tokens.auth0_access_token == "access_token"
        assert auth._tokens.auth0_refresh_token == "new_refresh_token"


@pytest.mark.asyncio
async def test_shark_auth_exchange_code_for_tokens_failure():
    """Test exchange_code_for_tokens with failure."""
    mock_config = MagicMock()
    mock_config.token_dir = "/tmp"
    mock_config.shark_region = "us"

    auth = SharkAuth(mock_config)

    # Mock aiohttp response
    mock_response = AsyncMock()
    mock_response.status = 400
    mock_response.json = AsyncMock(return_value={
        "error": "invalid_grant",
        "error_description": "Invalid authorization code"
    })

    with patch('aiohttp.ClientSession') as mock_session:
        mock_session_instance = AsyncMock()
        mock_session_instance.post = AsyncMock(return_value=mock_response)
        mock_session.return_value.__aenter__.return_value = mock_session_instance

        with pytest.raises(SharkAuthError):
            await auth.exchange_code_for_tokens("auth_code", "verifier")