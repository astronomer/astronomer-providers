"""This module contains a Google Cloud Storage hook."""
from typing import cast

from aiohttp import ClientSession as ClientSession
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from gcloud.aio.storage import Storage
from requests import Session

from astronomer.providers.google.common.hooks.base_google import GoogleBaseHookAsync

DEFAULT_TIMEOUT = 60


class GCSHookAsync(GoogleBaseHookAsync):
    """GCSHookAsync run on the trigger worker, inherits from GoogleBaseHookAsync"""

    sync_hook_class = GCSHook

    async def get_storage_client(self, session: ClientSession) -> Storage:
        """Returns a Google Cloud Storage service object."""
        with await self.service_file_as_context() as file:
            return Storage(service_file=file, session=cast(Session, session))
