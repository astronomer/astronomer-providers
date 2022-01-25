import asyncio
import logging
from typing import Any, Dict, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException
from astronomer_operators.google.cloud.hooks.gcs import GCSAsyncHook

log = logging.getLogger(__name__)


class GCSTrigger(BaseTrigger):
    """
    A trigger that fires and it finds the requested file or folder present in the given bucket.

    :param bucket: the bucket in the google cloud storage where the objects are residing.
    :type bucket: str
    :param object_name: the file or folder present in the bucket
    :type object_name: str
    :param google_cloud_conn_id: reference to the Google Connection
    :type google_cloud_conn_id: str
    :param polling_period_seconds: polling period in seconds to check for file/folder
    :type polling_period_seconds: float
    """

    def __init__(
        self,
        bucket: str,
        object_name: str,
        polling_period_seconds: float,
        google_cloud_conn_id: str,
    ):
        super().__init__()
        self.bucket = bucket
        self.object_name = object_name
        self.polling_period_seconds = polling_period_seconds
        self.google_cloud_conn_id: str = google_cloud_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes GCSTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.google.cloud.triggers.gcs.GCSTrigger",
            {
                "bucket": self.bucket,
                "object_name": self.object_name,
                "polling_period_seconds": self.polling_period_seconds,
                "google_cloud_conn_id": self.google_cloud_conn_id,
            },
        )

    async def run(self):
        """
        Simple loop until the relevant file/folder is found.
        """
        hook = self._get_async_hook()
        while True:
            try:
                self.log.info("Checking for object %s in bucket %s", self.object_name, self.bucket)
                res = await hook.exists(bucket_name=self.bucket,object_name=self.object_name)
                if res:
                    yield TriggerEvent({"status": "Success", "message": res})
                    return
                self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
                await asyncio.sleep(self.polling_period_seconds)
            except Exception as e:
                self.log.exception("Exception occurred while checking with error ", e)
                yield TriggerEvent({"status": "error", "message": e})
                return

    def _get_async_hook(self) -> GCSAsyncHook:
        return GCSAsyncHook(gcp_conn_id=self.google_cloud_conn_id)