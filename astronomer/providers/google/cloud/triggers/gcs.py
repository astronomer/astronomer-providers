import asyncio
import logging
from typing import Any, AsyncIterator, Dict, List, Tuple

from aiohttp import ClientSession as Session
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.google.cloud.hooks.gcs import GCSHookAsync

log = logging.getLogger(__name__)


class GCSBlobTrigger(BaseTrigger):
    """
    A trigger that fires and it finds the requested file or folder present in the given bucket.

    :param bucket: the bucket in the google cloud storage where the objects are residing.
    :param object_name: the file or folder present in the bucket
    :param google_cloud_conn_id: reference to the Google Connection
    :param polling_period_seconds: polling period in seconds to check for file/folder
    """

    def __init__(
        self,
        bucket: str,
        object_name: str,
        polling_period_seconds: float,
        google_cloud_conn_id: str,
        hook_params: dict,
    ):
        super().__init__()
        self.bucket = bucket
        self.object_name = object_name
        self.polling_period_seconds = polling_period_seconds
        self.google_cloud_conn_id: str = google_cloud_conn_id
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes GCSBlobTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger",
            {
                "bucket": self.bucket,
                "object_name": self.object_name,
                "polling_period_seconds": self.polling_period_seconds,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
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
                    hook=hook, bucket_name=self.bucket, object_name=self.object_name
                )
                if res == "success":
                    yield TriggerEvent({"status": "success", "message": res})
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    def _get_async_hook(self) -> GCSHookAsync:
        return GCSHookAsync(gcp_conn_id=self.google_cloud_conn_id, **self.hook_params)

    async def _object_exists(self, hook: GCSHookAsync, bucket_name: str, object_name: str) -> str:
        """
        Checks for the existence of a file in Google Cloud Storage.
        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the blob_name to check in the Google cloud
            storage bucket.
        """
        async with Session() as s:
            client = await hook.get_storage_client(s)
            bucket = client.get_bucket(bucket_name)
            object_response = await bucket.blob_exists(blob_name=object_name)
            if object_response:
                res = "success"
            else:
                res = "pending"
            return res


class GCSPrefixBlobTrigger(GCSBlobTrigger):
    """
     A trigger that fires and it looks for all the objects in the given bucket
     which matches the given prefix if not found sleep for certain interval and checks again.

    :param bucket: the bucket in the google cloud storage where the objects are residing.
    :param prefix: The prefix of the blob_names to match in the Google cloud storage bucket
    :param google_cloud_conn_id: reference to the Google Connection
    :param polling_period_seconds: polling period in seconds to check
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        polling_period_seconds: float,
        google_cloud_conn_id: str,
        hook_params: dict,
    ):
        super().__init__(
            bucket=bucket,
            object_name=prefix,
            polling_period_seconds=polling_period_seconds,
            google_cloud_conn_id=google_cloud_conn_id,
            hook_params=hook_params,
        )
        self.prefix = prefix

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes GCSBlobTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger",
            {
                "bucket": self.bucket,
                "prefix": self.prefix,
                "polling_period_seconds": self.polling_period_seconds,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Simple loop until the matches are found for the given prefix on the bucket..
        """
        try:
            hook = self._get_async_hook()
            while True:
                res = await self._object_with_prefix_exists(
                    hook=hook, bucket_name=self.bucket, prefix=self.prefix
                )
                if len(res) > 0:
                    yield TriggerEvent(
                        {"status": "success", "message": "Successfully completed", "matches": res}
                    )
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    async def _object_with_prefix_exists(
        self, hook: GCSHookAsync, bucket_name: str, prefix: str
    ) -> List[str]:
        """
        Returns list of blobs which matches the given prefix for the given bucket.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param prefix: The prefix of the blob_names to match in the Google cloud
            storage bucket.
        """
        async with Session() as s:
            client = await hook.get_storage_client(s)
            bucket = client.get_bucket(bucket_name)
            object_response = await bucket.list_blobs(prefix=prefix)
            return object_response
