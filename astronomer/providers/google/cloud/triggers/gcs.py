import asyncio
import logging
from datetime import datetime
from typing import Any, AsyncIterator, Dict, Tuple

from aiohttp import ClientSession as Session
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone

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


class GCSCheckBlobUpdateTimeTrigger(BaseTrigger):
    """
    A trigger Makes an async call to GCS and check whether the object is updated in blob.

    :param bucket: google cloud storage bucket name cloud storage where the objects are residing.
    :param object_name: the file or folder present in the bucket
    :param ts: datetime object
    :param polling_period_seconds: polling period in seconds to check for file/folder
    :param google_cloud_conn_id: reference to the Google Connection
    :param hook_params: DIct object has delegate_to and impersonation_chain
    """

    def __init__(
        self,
        bucket: str,
        object_name: str,
        ts: datetime,
        polling_period_seconds: float,
        google_cloud_conn_id: str,
        hook_params: dict,
    ):
        super().__init__()
        self.bucket = bucket
        self.object_name = object_name
        self.ts = ts
        self.polling_period_seconds = polling_period_seconds
        self.google_cloud_conn_id: str = google_cloud_conn_id
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes GCSCheckBlobUpdateTimeTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger",
            {
                "bucket": self.bucket,
                "object_name": self.object_name,
                "ts": self.ts,
                "polling_period_seconds": self.polling_period_seconds,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        try:
            hook = self._get_async_hook()
            while True:
                status, res = await self._is_blob_updated_after(
                    hook=hook, bucket_name=self.bucket, object_name=self.object_name, ts=self.ts
                )
                if status:
                    yield TriggerEvent(res)
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    def _get_async_hook(self) -> GCSHookAsync:
        return GCSHookAsync(gcp_conn_id=self.google_cloud_conn_id, **self.hook_params)

    async def _is_blob_updated_after(
        self, hook: GCSHookAsync, bucket_name: str, object_name: str, ts: datetime
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Checks if object in blob object is updated.

        :hook: GCSHookAsync Hook class
        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param object_name: The name of the blob_name to check in the Google cloud
            storage bucket.
        """
        async with Session() as s:
            client = await hook.get_storage_client(s)
            bucket = client.get_bucket(bucket_name)
            blob = await bucket.get_blob(blob_name=object_name)
            if blob is None:
                res = {
                    "message": f"Object ({object_name}) not found in Bucket ({bucket_name})",
                    "status": "error",
                }
                return True, res
            """
             Blob updated time is in string format so converting the string datetime to datetime object to
             compare the last updated time
            """
            blob_updated_time = datetime.strptime(blob.updated, '%Y-%m-%dT%H:%M:%S.%fZ').replace(
                tzinfo=timezone.utc
            )
            if blob_updated_time is not None:
                if not ts.tzinfo:
                    ts = ts.replace(tzinfo=timezone.utc)
                self.log.info("Verify object date: %s > %s", blob_updated_time, ts)
                if blob_updated_time > ts:
                    return True, {"status": "success", "message": "success"}
            return False, {"status": "pending", "message": "pending"}
