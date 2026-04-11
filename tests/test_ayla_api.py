"""Test Ayla API."""
from unittest.mock import AsyncMock, patch

import pytest

from src.ayla_api import AylaApi

from .conftest import shark_auth


@pytest.mark.asyncio
async def test_ayla_api_init():
    """Test AylaAPI initialization."""
    api = AylaApi(shark_auth)
    assert api.config.shark_username == shark_auth.config.shark_username
    assert api.config.shark_password == shark_auth.config.shark_password

