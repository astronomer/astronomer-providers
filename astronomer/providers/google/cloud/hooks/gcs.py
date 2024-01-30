"""This module contains a Google Cloud Storage hook."""

import warnings
from typing import Any, cast

from aiohttp import ClientSession as ClientSession
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from gcloud.aio.storage import Storage
from requests import Session

from astronomer.providers.google.common.hooks.base_google import GoogleBaseHookAsync

DEFAULT_TIMEOUT = 60


class GCSHookAsync(GoogleBaseHookAsync):
    """GCSHookAsync run on the trigger worker, inherits from GoogleBaseHookAsync

    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.google.cloud.hooks.gcs.GCSAsyncHook` instead
    """

    def __init__(self, **kwargs: Any):
        warnings.warn(
            (
                "This class is deprecated and will be removed in 2.0.0."
                "Use `airflow.providers.google.cloud.hooks.gcs.GCSAsyncHook` instead"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)

    sync_hook_class = GCSHook

    async def get_storage_client(self, session: ClientSession) -> Storage:
        """Returns a Google Cloud Storage service object."""
        with await self.service_file_as_context() as file:
            return Storage(service_file=file, session=cast(Session, session))
