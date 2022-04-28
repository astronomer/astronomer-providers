from unittest import mock

import pytest
from gcloud.aio.storage import Storage

from astronomer.providers.google.cloud.hooks.gcs import GCSHookAsync


@pytest.mark.asyncio
@mock.patch("aiohttp.client.ClientSession")
async def test_get_job_status(mock_session):
    hook = GCSHookAsync()
    result = await hook.get_storage_client(mock_session)
    assert isinstance(result, Storage)
