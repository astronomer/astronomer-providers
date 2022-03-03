"""This module contains a Azure Cloud Storage hook."""

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from azure.storage.blob.aio import BlobServiceClient
from astronomer.providers.microsoft.common.hooks.base_azure import AzureBaseHookAsync

DEFAULT_TIMEOUT = 60

class WasbHookAsync(AzureBaseHookAsync):
    sync_hook_class = WasbHook

    async def get_blob_service_client(self) -> BlobServiceClient:
        """
        Returns an Azure Cloud Storage containerclient object.
        """
        with await self.get_conn() as conn:
            return conn
