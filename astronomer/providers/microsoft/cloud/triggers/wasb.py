import asyncio
import logging
from typing import Any, Dict, Tuple

from aiohttp import ClientSession as Session
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.microsoft.cloud.hooks.wasb import WasbHookAsync

log = logging.getLogger(__name__)


class WasbBlobTrigger(BaseTrigger):
    """
    A trigger that fires and it finds the requested file or folder present in the given bucket.
    :param container_name: the container in the azure cloud storage where the blobs are residing.
    :param blob_name: the file or folder present in the container
    :param wasb_conn_id: reference to the Azure Connection
    :param polling_period_seconds: polling period in seconds to check for file/folder
    """

    def __init__(
        self,
        container_name: str,
        blob_name: str,
        polling_period_seconds: float,
        wasb_conn_id: str
    ):
        super().__init__()
        self.container_name = container_name
        self.blob_name = blob_name
        self.polling_period_seconds = polling_period_seconds
        self.wasb_conn_id: str = wasb_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes GCSBlobTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.microsoft.cloud.triggers.wasb.WasbBlobTrigger",
            {
                "container_name": self.container_name,
                "blob_name": self.blob_name,
                "polling_period_seconds": self.polling_period_seconds,
                "wasb_conn_id": self.wasb_conn_id
            },
        )

    async def run(self):
        """
        Simple loop until the relevant file/folder is found.
        """
        try:
            hook = self._get_async_hook()
            while True:
                res = await self._object_exists(
                    hook=hook, container_name=self.container_name, blob_name=self.blob_name
                )
                if res == "success":
                    yield TriggerEvent({"status": "success", "message": res})
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    def _get_async_hook(self) -> WasbHookAsync:
        return WasbHookAsync(wasb_conn_id=self.wasb_conn_id)

    async def _object_exists(self, hook: WasbHookAsync, container_name: str, blob_name: str) -> str:
        """
        Checks for the existence of a file in Azure Cloud Storage.
        :param container_name: The Azure Cloud Storage container where the blob is.
        :param blob_name: The name of the blob to check in the Azure cloud
            storage container.
        """
        async with Session() as s:
            client = await hook.get_blob_service_client()
            container = client.get_container_client(container=container_name)
            object_response = await container.get_blob_client(blob=blob_name).get_blob_properties()
            if object_response:
                res = "success"
            else:
                res = "pending"
            return res