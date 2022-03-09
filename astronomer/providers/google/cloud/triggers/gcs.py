import asyncio
import logging
import os
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional, Set, Tuple

from aiohttp import ClientSession
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
        hook_params: Dict[str, Any],
    ):
        super().__init__()
        self.bucket = bucket
        self.object_name = object_name
        self.polling_period_seconds = polling_period_seconds
        self.google_cloud_conn_id: str = google_cloud_conn_id
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes GCSBlobTrigger arguments and classpath."""
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

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple loop until the relevant file/folder is found."""
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
        async with ClientSession() as s:
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
        hook_params: Dict[str, Any],
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
        """Serializes GCSPrefixBlobTrigger arguments and classpath."""
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
        """Simple loop until the matches are found for the given prefix on the bucket."""
        try:
            hook = self._get_async_hook()
            while True:
                res = await self._list_blobs_with_prefix(
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

    async def _list_blobs_with_prefix(self, hook: GCSHookAsync, bucket_name: str, prefix: str) -> List[str]:
        """
        Returns list of blobs which matches the given prefix for the given bucket.

        :param bucket_name: The Google Cloud Storage bucket where the object is.
        :param prefix: The prefix of the blob_names to match in the Google cloud
            storage bucket.
        """
        async with ClientSession() as session:
            client = await hook.get_storage_client(session)
            bucket = client.get_bucket(bucket_name)
            object_response = await bucket.list_blobs(prefix=prefix)
            return object_response


class GCSUploadSessionTrigger(GCSPrefixBlobTrigger):
    """
    Checks for changes in the number of objects at prefix in Google Cloud Storage
    bucket and returns Trigger Event if the inactivity period has passed with no
    increase in the number of objects.

    :param bucket: The Google Cloud Storage bucket where the objects are.
        expected.
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :param polling_period_seconds: polling period in seconds to check
    :param inactivity_period: The total seconds of inactivity to designate
        an upload session is over. Note, this mechanism is not real time and
        this operator may not return until a interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for upload session
        to be considered valid.
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
        between intervals valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :param google_cloud_conn_id: The connection ID to use when connecting
        to Google Cloud Storage.
    """

    def __init__(
        self,
        bucket: str,
        prefix: str,
        polling_period_seconds: float,
        google_cloud_conn_id: str,
        hook_params: Dict[str, Any],
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: Optional[Set[str]] = None,
        allow_delete: bool = True,
    ):
        super().__init__(
            bucket=bucket,
            prefix=prefix,
            polling_period_seconds=polling_period_seconds,
            google_cloud_conn_id=google_cloud_conn_id,
            hook_params=hook_params,
        )
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects if previous_objects else set()
        self.inactivity_seconds = 0.0
        self.allow_delete = allow_delete
        self.last_activity_time: Optional[datetime] = None

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes GCSUploadSessionTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger",
            {
                "bucket": self.bucket,
                "prefix": self.prefix,
                "polling_period_seconds": self.polling_period_seconds,
                "google_cloud_conn_id": self.google_cloud_conn_id,
                "hook_params": self.hook_params,
                "inactivity_period": self.inactivity_period,
                "min_objects": self.min_objects,
                "previous_objects": self.previous_objects,
                "allow_delete": self.allow_delete,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Simple loop until no change in any new files or deleted in list blob is
        found for the inactivity_period.
        """
        try:
            hook = self._get_async_hook()
            while True:
                list_blobs = await self._list_blobs_with_prefix(
                    hook=hook, bucket_name=self.bucket, prefix=self.prefix
                )
                res = self._is_bucket_updated(set(list_blobs))
                if res["status"] == "success":
                    yield TriggerEvent(res)
                    return
                elif res["status"] == "error":
                    yield TriggerEvent(res)
                    return
                await asyncio.sleep(self.polling_period_seconds)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return

    def _get_time(self) -> datetime:
        """
        This is just a wrapper of datetime.datetime.now to simplify mocking in the
        unittests.
        """
        return datetime.now()

    def _is_bucket_updated(self, current_objects: Set[str]) -> Dict[str, str]:
        """
        Checks whether new objects have been uploaded and the inactivity_period
        has passed and updates the state of the sensor accordingly.

        :param current_objects: set of object ids in bucket during last check.
        """
        current_num_objects = len(current_objects)
        if current_objects > self.previous_objects:
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next check interval.
            self.log.info(
                "New objects found at %s resetting last_activity_time.",
                os.path.join(self.bucket, self.prefix),
            )
            self.log.debug("New objects: %s", "\n".join(current_objects - self.previous_objects))
            self.last_activity_time = self._get_time()
            self.inactivity_seconds = 0
            self.previous_objects = current_objects
            return {"status": "pending"}

        if self.previous_objects - current_objects:
            # During the last interval check objects were deleted.
            if self.allow_delete:
                self.previous_objects = current_objects
                self.last_activity_time = self._get_time()
                self.log.warning(
                    "%s Objects were deleted during the last interval."
                    " Updating the file counter and resetting last_activity_time.",
                    self.previous_objects - current_objects,
                )
                return {"status": "pending"}
            return {
                "status": "error",
                "message": "Illegal behavior: objects were deleted in between check intervals",
            }
        if self.last_activity_time:
            self.inactivity_seconds = (self._get_time() - self.last_activity_time).total_seconds()
        else:
            # Handles the first check where last inactivity time is None.
            self.last_activity_time = self._get_time()
            self.inactivity_seconds = 0

        if self.inactivity_seconds >= self.inactivity_period:
            path = os.path.join(self.bucket, self.prefix)

            if current_num_objects >= self.min_objects:
                success_message = (
                    "SUCCESS: Sensor found %s objects at %s. Waited at least %s "
                    "seconds, with no new objects dropped."
                )
                self.log.info(success_message, current_num_objects, path, self.inactivity_seconds)
                return {
                    "status": "success",
                    "message": success_message % (current_num_objects, path, self.inactivity_seconds),
                }

            error_message = "FAILURE: Inactivity Period passed, not enough objects found in %s"
            self.log.error(error_message, path)
            return {"status": "error", "message": error_message % path}
        return {"status": "pending"}
