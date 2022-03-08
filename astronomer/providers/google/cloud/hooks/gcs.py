"""This module contains a Google Cloud Storage hook."""

from aiohttp import ClientSession as Session
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from gcloud.aio.storage import Storage

from astronomer.providers.google.common.hooks.base_google import GoogleBaseHookAsync

DEFAULT_TIMEOUT = 60


class GCSHookAsync(GoogleBaseHookAsync):
    sync_hook_class = GCSHook

    async def get_storage_client(self, session: Session) -> Storage:
        """
        Returns a Google Cloud Storage service object.
        """
        with await self.service_file_as_context() as file:
            return Storage(service_file=file, session=session)
