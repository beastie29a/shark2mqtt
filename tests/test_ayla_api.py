"""Test Ayla API."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, PropertyMock, patch

import pytest

from src.ayla_api import AylaApi
from src.exc import AylaApiError, SharkAuthError
from src.shark_device import SharkVacuum


@pytest.mark.asyncio
async def test_ayla_api_init(mock_auth):
    """Test AylaAPI initialization."""
    api = AylaApi(mock_auth)
    assert api.config.shark_username == mock_auth.config.shark_username
    assert api.config.shark_password == mock_auth.config.shark_password


@pytest.mark.asyncio
async def test_get_session(mock_auth):
    """Test _get_session method."""
    api = AylaApi(mock_auth)
    session = await api._get_session()
    assert session is not None
    assert not session.closed


@pytest.mark.asyncio
async def test_close(mock_auth):
    """Test close method."""
    api = AylaApi(mock_auth)
    session = await api._get_session()
    await api.close()
    assert session.closed


@pytest.mark.asyncio
async def test_token_expiring_soon(mock_auth):
    """Test token_expiring_soon property."""
    api = AylaApi(mock_auth)

    # No token expiry set
    assert api.token_expiring_soon is True

    # Set expiry in future

    api._token_expiry = datetime.now(UTC) + timedelta(minutes=10)
    assert api.token_expiring_soon is False

    # Set expiry in past
    api._token_expiry = datetime.now(UTC) - timedelta(minutes=10)
    assert api.token_expiring_soon is True


# Example of the correct fix for test_sign_in:
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
@patch("src.shark_auth.SharkAuth.update_ayla_tokens")
async def test_sign_in(mock_update_tokens, mock_post, mock_auth):
    """Test sign_in method."""
    api = AylaApi(mock_auth)

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(
        return_value={"access_token": "test_access_token", "refresh_token": "test_refresh_token", "expires_in": 3600}
    )

    mock_post.return_value.__aenter__.return_value = mock_response
    await api.sign_in("test_id_token")

    assert api._access_token == "test_access_token"
    assert api._refresh_token == "test_refresh_token"
    assert api._token_expiry is not None


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
@patch("src.shark_auth.SharkAuth.update_ayla_tokens")
async def test_refresh_auth(mock_update_tokens, mock_post, mock_auth):
    """Test refresh_auth method."""
    api = AylaApi(mock_auth)
    api._refresh_token = "test_refresh_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(
        return_value={"access_token": "new_access_token", "refresh_token": "new_refresh_token", "expires_in": 3600}
    )

    mock_post.return_value.__aenter__.return_value = mock_response

    await api.refresh_auth()

    assert api._access_token == "new_access_token"
    assert api._refresh_token == "new_refresh_token"
    assert api._token_expiry is not None


@pytest.mark.asyncio
async def test_refresh_auth_no_refresh_token(mock_auth):
    """Test refresh_auth method when no refresh token is available."""
    api = AylaApi(mock_auth)
    api._refresh_token = None
    # Mock the _auth.ayla_refresh_token property to avoid the JSON serialization issue
    with patch.object(mock_auth, 'ayla_refresh_token', None):
        with pytest.raises(SharkAuthError, match="No Ayla refresh token available"):
            await api.refresh_auth()


@pytest.mark.asyncio
async def test_ensure_ayla_auth_with_existing_token(mock_auth):
    """Test _ensure_ayla_auth method with existing token."""
    api = AylaApi(mock_auth)

    # Test with existing token
    api._access_token = "test_token"
    api._refresh_token = "test_refresh_token"
    api._token_expiry = datetime.now(UTC) + timedelta(minutes=10)

    # Mock refresh_auth to avoid actual refresh
    with patch.object(api, "refresh_auth") as mock_refresh:
        await api._ensure_ayla_auth()
        mock_refresh.assert_not_called()


@pytest.mark.asyncio
async def test_ensure_ayla_auth_refresh_needed(mock_auth):
    """Test _ensure_ayla_auth method when refresh is needed."""
    api = AylaApi(mock_auth)

    # Set token to expire soon

    api._token_expiry = datetime.now(UTC) - timedelta(minutes=10)

    # Mock refresh_auth to avoid actual refresh
    with patch.object(api, "refresh_auth") as mock_refresh:
        with patch.object(api, "sign_in") as mock_sign_in:
            await api._ensure_ayla_auth()
            # Should call refresh_auth, not sign_in
            mock_refresh.assert_called_once()
            mock_sign_in.assert_not_called()


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.request")
async def test_request_success(mock_request, mock_auth):
    """Test _request method success."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"
    api._refresh_token = "test_refresh_token"
    api._token_expiry = datetime.now(UTC) + timedelta(minutes=10)

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={"test": "data"})
    mock_request.return_value.__aenter__.return_value = mock_response

    result = await api._request("GET", "http://test.com")
    assert result == {"test": "data"}


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.request")
async def test_request_401(mock_request, mock_auth):
    """Test _request method handling of 401 error."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and responses
    mock_response_401 = AsyncMock()
    mock_response_401.status = 401
    mock_response_401.text = AsyncMock(return_value="Unauthorized")

    mock_response_200 = AsyncMock()
    mock_response_200.status = 200
    mock_response_200.json = AsyncMock(return_value={"test": "data"})

    # mock_request.call_count = 0
    mock_request.return_value.__aenter__.side_effect = [mock_response_401, mock_response_200]

    with patch.object(api, "_ensure_ayla_auth", return_value=None):
        with patch.object(api, "refresh_auth", return_value=None):
            result = await api._request("GET", "http://test.com")
            assert mock_request.call_count == 2
            assert result == {"test": "data"}


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.request")
async def test_request_error(mock_request, mock_auth):
    """Test _request method handling of error response."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Internal Server Error")
    mock_request.return_value.__aenter__.return_value = mock_response

    with patch.object(api, "_ensure_ayla_auth", return_value=None):
        with pytest.raises(AylaApiError, match="Ayla API error \\(500\\): Internal Server Error"):
            await api._request("GET", "http://test.com")


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.request")
async def test_list_devices(mock_request, mock_auth):
    """Test list_devices method."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(
        return_value=[
            {"device": {"dsn": "test_dsn1", "name": "Vacuum1"}},
            {"device": {"dsn": "test_dsn2", "name": "Vacuum2"}},
        ]
    )

    mock_request.return_value.__aenter__.return_value = mock_response

    with patch.object(api, "_ensure_ayla_auth", return_value=None):
        result = await api.list_devices()
        assert len(result) == 2
        assert result[0]["dsn"] == "test_dsn1"
        assert result[1]["dsn"] == "test_dsn2"


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.request")
async def test_get_device_properties(mock_request, mock_auth):
    """Test get_device_properties method."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value=[{"property": {"name": "test_prop", "value": "test_value"}}])

    mock_request.return_value.__aenter__.return_value = mock_response

    with patch.object(api, "_ensure_ayla_auth", return_value=None):
        result = await api.get_device_properties("test_dsn")
        assert len(result) == 1
        assert result[0]["property"]["name"] == "test_prop"


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.request")
async def test_set_device_property(mock_request, mock_auth):
    """Test set_device_property method."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200

    mock_request.return_value.__aenter__.return_value = mock_response

    with patch.object(api, "_ensure_ayla_auth", return_value=None):
        await api.set_device_property("test_dsn", "test_prop", "test_value")
        # Check that request was called
        assert mock_request.called


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.request")
async def test_get_devices(mock_request, mock_auth):
    """Test get_devices method."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and responses
    mock_response_devices = AsyncMock()
    mock_response_devices.status = 200
    mock_response_devices.json = AsyncMock(return_value=[{"device": {"dsn": "test_dsn", "name": "Vacuum"}}])

    mock_response_props = AsyncMock()
    mock_response_props.status = 200
    mock_response_props.json = AsyncMock(
        return_value=[
            {"property": {"name": "GET_Robot_Room_List", "value": PropertyMock(return_value="1:Living:Kitchen")}},
            {"property": {"name": "SET_AreasToClean_V3", "value": PropertyMock(return_value="test")}},
        ]
    )

    mock_request.return_value.__aenter__.side_effect = [mock_response_devices, mock_response_props]

    with patch.object(api, "_ensure_ayla_auth", return_value=None):
        result = await api.get_devices()
        assert len(result) == 1
        assert isinstance(result[0], SharkVacuum)
        assert result[0].floor_id == "1"
        assert result[0].rooms == ["Living", "Kitchen"]
        assert result[0].has_areas_v3 is True


@pytest.mark.asyncio
@patch("aiohttp.ClientSession.post")
@patch("aiohttp.ClientSession.request")
async def test_send_command(mock_request, mock_post, mock_auth):
    """Test send_command method."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200

    mock_request.return_value.__aenter__.return_value = mock_response
    mock_post.return_value.__aenter__.return_value = mock_response

    with patch.object(api, "_ensure_ayla_auth", return_value=None):
        await api.send_command("test_dsn", "start")
        # Check that request was called
        assert mock_request.called


@pytest.mark.asyncio
async def test_send_command_unknown(mock_auth):
    """Test send_command method with unknown command."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_session = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_session.request = AsyncMock(return_value=mock_response)

    with (
        patch.object(api, "_get_session", return_value=mock_session),
        patch.object(api, "_ensure_ayla_auth", return_value=None),
    ):
        await api.send_command("test_dsn", "unknown_command")
        # Should not call request for unknown command
        assert not mock_session.request.called


@pytest.mark.asyncio
async def test_set_fan_speed(mock_auth):
    """Test set_fan_speed method."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={})

    with patch("aiohttp.ClientSession.request") as mock_request:
        mock_request.return_value.__aenter__.return_value = mock_response
        with patch.object(api, "_ensure_ayla_auth", return_value=None):
            await api.set_fan_speed("test_dsn", "normal")
            # Check that request was called
            assert mock_request.called


@pytest.mark.asyncio
async def test_set_fan_speed_unknown(mock_auth):
    """Test set_fan_speed method with unknown speed."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_session = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_session.request = AsyncMock(return_value=mock_response)

    with (
        patch.object(api, "_get_session", return_value=mock_session),
        patch.object(api, "_ensure_ayla_auth", return_value=None),
    ):
        await api.set_fan_speed("test_dsn", "unknown_speed")
        # Should not call request for unknown speed
        assert not mock_session.request.called


@pytest.mark.asyncio
async def test_clean_rooms_v3(mock_auth):
    """Test clean_rooms method with V3 format."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={})

    with patch("aiohttp.ClientSession.request") as mock_request:
        mock_request.return_value.__aenter__.return_value = mock_response
        with patch.object(api, "_ensure_ayla_auth", return_value=None):
            await api.clean_rooms(
                "test_dsn", ["Living", "Kitchen"], "1", clean_type="dry", clean_count=1, mode="UserRoom", use_v3=True
            )
            # Check that request was called
            assert mock_request.called


@pytest.mark.asyncio
async def test_clean_rooms_legacy(mock_auth):
    """Test clean_rooms method with legacy format."""
    api = AylaApi(mock_auth)
    api._access_token = "test_token"

    # Mock the session and response
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={})

    with patch("aiohttp.ClientSession.request") as mock_request:
        mock_request.return_value.__aenter__.return_value = mock_response
        with patch.object(api, "_ensure_ayla_auth", return_value=None):
            await api.clean_rooms(
                "test_dsn", ["Living", "Kitchen"], "1", clean_type="dry", clean_count=1, mode="UserRoom", use_v3=False
            )
            # Check that request was called
            assert mock_request.called


