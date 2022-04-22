import asyncio
from datetime import datetime, timedelta
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent
from gcloud.aio.storage import Bucket, Storage

from astronomer.providers.google.cloud.hooks.gcs import GCSHookAsync
from astronomer.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSCheckBlobUpdateTimeTrigger,
    GCSPrefixBlobTrigger,
    GCSUploadSessionTrigger,
)

TEST_BUCKET = "TEST_BUCKET"
TEST_OBJECT = "TEST_OBJECT"
TEST_PREFIX = "TEST_PREFIX"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_POLLING_INTERVAL = 3.0
TEST_DAG_ID = "unit_tests_gcs_sensor"
TEST_HOOK_PARAMS = {}
TEST_INACTIVITY_PERIOD = 5.0
TEST_MIN_OBJECTS = 1
TEST_ALLOW_DELETE = True
TEST_PREVIOUS_OBJECTS = {"a", "ab"}
TEST_TS_OBJECT = datetime.utcnow()


def test_gcs_blob_trigger_serialization():
    """
    Asserts that the GCSBlobTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger"
    assert kwargs == {
        "bucket": TEST_BUCKET,
        "object_name": TEST_OBJECT,
        "polling_period_seconds": TEST_POLLING_INTERVAL,
        "google_cloud_conn_id": TEST_GCP_CONN_ID,
        "hook_params": TEST_HOOK_PARAMS,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_blob_trigger_success(mock_object_exists):
    """
    Tests that the GCSBlobTrigger is success case
    """
    mock_object_exists.return_value = "success"

    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "success", "message": "success"}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_blob_trigger_pending(mock_object_exists):
    """
    Test that GCSBlobTrigger is in loop if file isn't found.
    """
    mock_object_exists.return_value = "pending"

    trigger = GCSBlobTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSBlobTrigger._object_exists")
async def test_gcs_blob_trigger_exception(mock_object_exists):
    """
    Tests the GCSBlobTrigger does fire if there is an exception.
    """
    mock_object_exists.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
    trigger = GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exists,response",
    [
        (True, "success"),
        (False, "pending"),
    ],
)
async def test_object_exists(exists, response):
    """
    Tests to check if a particular object in Google Cloud Storage
    is found or not
    """
    hook = mock.AsyncMock(GCSHookAsync)
    storage = mock.AsyncMock(Storage)
    hook.get_storage_client.return_value = storage
    bucket = mock.AsyncMock(Bucket)
    storage.get_bucket.return_value = bucket
    bucket.blob_exists.return_value = exists
    trigger = GCSBlobTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    res = await trigger._object_exists(hook, TEST_BUCKET, TEST_OBJECT)
    assert res == response
    bucket.blob_exists.assert_called_once_with(blob_name=TEST_OBJECT)


def test_gcs_prefix_blob_trigger_serialization():
    """
    Asserts that the GCSPrefixBlobTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = GCSPrefixBlobTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger"
    assert kwargs == {
        "bucket": TEST_BUCKET,
        "prefix": TEST_PREFIX,
        "polling_period_seconds": TEST_POLLING_INTERVAL,
        "google_cloud_conn_id": TEST_GCP_CONN_ID,
        "hook_params": TEST_HOOK_PARAMS,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger._list_blobs_with_prefix")
async def test_gcs_prefix_blob_trigger_success(mock_list_blobs_with_prefixs):
    """
    Tests that the GCSPrefixBlobTrigger is success case
    """
    mock_list_blobs_with_prefixs.return_value = ["success"]

    trigger = GCSPrefixBlobTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert (
        TriggerEvent({"status": "success", "message": "Successfully completed", "matches": ["success"]})
        in task
    )


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger._list_blobs_with_prefix")
async def test_gcs_prefix_blob_trigger_exception(mock_list_blobs_with_prefixs):
    """
    Tests the GCSPrefixBlobTrigger does fire if there is an exception.
    """
    mock_list_blobs_with_prefixs.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
    trigger = GCSPrefixBlobTrigger(
        bucket=TEST_BUCKET,
        prefix=TEST_PREFIX,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSPrefixBlobTrigger._list_blobs_with_prefix")
async def test_gcs_prefix_blob_trigger_pending(mock_list_blobs_with_prefixs):
    """
    Test that GCSPrefixBlobTrigger is in loop if file isn't found.
    """
    mock_list_blobs_with_prefixs.return_value = []

    trigger = GCSPrefixBlobTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
async def test_list_blobs_with_prefix():
    """
    Tests to check if a particular object in Google Cloud Storage
    is found or not
    """
    hook = mock.AsyncMock(GCSHookAsync)
    storage = mock.AsyncMock(Storage)
    hook.get_storage_client.return_value = storage
    bucket = mock.AsyncMock(Bucket)
    storage.get_bucket.return_value = bucket
    bucket.list_blobs.return_value = ["test_string"]
    trigger = GCSPrefixBlobTrigger(
        bucket=TEST_BUCKET,
        prefix=TEST_PREFIX,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    res = await trigger._list_blobs_with_prefix(hook, TEST_BUCKET, TEST_PREFIX)
    assert res == ["test_string"]
    bucket.list_blobs.assert_called_once_with(prefix=TEST_PREFIX)


def test_gcs_upload_session_trigger_serialization():
    """
    Asserts that the GCSUploadSessionTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = GCSUploadSessionTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        TEST_INACTIVITY_PERIOD,
        TEST_MIN_OBJECTS,
        TEST_PREVIOUS_OBJECTS,
        TEST_ALLOW_DELETE,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger"
    assert kwargs == {
        "bucket": TEST_BUCKET,
        "prefix": TEST_PREFIX,
        "polling_period_seconds": TEST_POLLING_INTERVAL,
        "google_cloud_conn_id": TEST_GCP_CONN_ID,
        "hook_params": TEST_HOOK_PARAMS,
        "inactivity_period": TEST_INACTIVITY_PERIOD,
        "min_objects": TEST_MIN_OBJECTS,
        "previous_objects": TEST_PREVIOUS_OBJECTS,
        "allow_delete": TEST_ALLOW_DELETE,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger._list_blobs_with_prefix")
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger._is_bucket_updated")
async def test_gcs_upload_session_trigger_pending(mock_is_bucket_updated, mock_list_blobs):
    """
    Test that GCSUploadSessionTrigger is in loop if Upload is still in progress till inactivity period.
    """
    mock_is_bucket_updated.return_value = {"status": "pending"}
    mock_list_blobs.return_value = TEST_PREVIOUS_OBJECTS

    trigger = GCSUploadSessionTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        TEST_INACTIVITY_PERIOD,
        TEST_MIN_OBJECTS,
        TEST_PREVIOUS_OBJECTS,
        TEST_ALLOW_DELETE,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "is_bucket_return_value",
    [
        ({"status": "success", "message": "Successfully completed"}),
        ({"status": "error", "message": "Error occurred"}),
    ],
)
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger._list_blobs_with_prefix")
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger._is_bucket_updated")
async def test_gcs_upload_session_trigger_success(
    mock_is_bucket_updated, mock_list_blobs, is_bucket_return_value
):
    """
    Tests that the GCSUploadSessionTrigger is success case
    """
    mock_is_bucket_updated.return_value = is_bucket_return_value
    mock_list_blobs.return_value = TEST_PREVIOUS_OBJECTS

    trigger = GCSUploadSessionTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        TEST_INACTIVITY_PERIOD,
        TEST_MIN_OBJECTS,
        TEST_PREVIOUS_OBJECTS,
        TEST_ALLOW_DELETE,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent(is_bucket_return_value) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger._list_blobs_with_prefix")
async def test_gcs_upload_session_trigger_exception(mock_list_blobs):
    """
    Tests the GCSUploadSessionTrigger does fire if there is an exception.
    """
    mock_list_blobs.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
    trigger = GCSUploadSessionTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        TEST_INACTIVITY_PERIOD,
        TEST_MIN_OBJECTS,
        TEST_PREVIOUS_OBJECTS,
        TEST_ALLOW_DELETE,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "allow_delete, current_objects, response",
    [
        (True, {"a", "aa", "ab"}, {"status": "pending"}),
        (True, {"a"}, {"status": "pending"}),
        (
            False,
            {"a"},
            {
                "status": "error",
                "message": "Illegal behavior: objects were deleted in between check intervals",
            },
        ),
    ],
)
async def test_is_bucket_updated_pending_status(allow_delete, current_objects, response):
    """
    Tests to check if there is less items or more items than expected and reset the inactivity period
    along with the proper status
    """

    trigger = GCSUploadSessionTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        TEST_INACTIVITY_PERIOD,
        TEST_MIN_OBJECTS,
        TEST_PREVIOUS_OBJECTS,
        allow_delete,
    )
    res = trigger._is_bucket_updated(current_objects=current_objects)
    assert res == response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "last_activity_time , min_objects, response",
    [
        (
            datetime.now() - timedelta(hours=0, minutes=50),
            10,
            {
                "status": "error",
                "message": (
                    "FAILURE: Inactivity Period passed, not enough objects found in TEST_BUCKET/TEST_PREFIX"
                ),
            },
        ),
        (None, 10, {"status": "pending"}),
        (
            datetime.now() - timedelta(hours=0, minutes=50),
            1,
            {
                "status": "success",
                "message": (
                    "SUCCESS: Sensor found 2 objects at TEST_BUCKET/TEST_PREFIX. "
                    "Waited at least 5.0 seconds, with no new objects dropped."
                ),
            },
        ),
    ],
)
@mock.patch("astronomer.providers.google.cloud.triggers.gcs.GCSUploadSessionTrigger._get_time")
async def test_is_bucket_updated_success_failure_status(mock_time, last_activity_time, min_objects, response):
    """
    Tests to check if inactivity period is finished and found min objects or not and return status
    based on that.
    """
    mock_time.return_value = (
        last_activity_time + timedelta(seconds=5) if last_activity_time else datetime.now()
    )

    trigger = GCSUploadSessionTrigger(
        TEST_BUCKET,
        TEST_PREFIX,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        TEST_INACTIVITY_PERIOD,
        min_objects,
        TEST_PREVIOUS_OBJECTS,
        TEST_ALLOW_DELETE,
    )
    trigger.last_activity_time = last_activity_time
    res = trigger._is_bucket_updated(current_objects=TEST_PREVIOUS_OBJECTS)
    assert res == response


def test_gcs_blob_update_trigger_serialization():
    """
    Asserts that the GCSCheckBlobUpdateTimeTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = GCSCheckBlobUpdateTimeTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_TS_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger"
    assert kwargs == {
        "bucket": TEST_BUCKET,
        "object_name": TEST_OBJECT,
        "ts": TEST_TS_OBJECT,
        "polling_period_seconds": TEST_POLLING_INTERVAL,
        "google_cloud_conn_id": TEST_GCP_CONN_ID,
        "hook_params": TEST_HOOK_PARAMS,
    }


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger._is_blob_updated_after"
)
async def test_gcs_blob_update_trigger_success(mock_blob_updated):
    """
    Tests success case GCSCheckBlobUpdateTimeTrigger
    """
    mock_blob_updated.return_value = True, {"status": "success", "message": "success"}

    trigger = GCSCheckBlobUpdateTimeTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_TS_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "success", "message": "success"}) in task


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger._is_blob_updated_after"
)
async def test_gcs_blob_update_trigger_pending(mock_blob_updated):
    """
    Test that GCSCheckBlobUpdateTimeTrigger is in loop till file isn't updated.
    """
    mock_blob_updated.return_value = False, {"status": "pending", "message": "pending"}

    trigger = GCSCheckBlobUpdateTimeTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_TS_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.google.cloud.triggers.gcs.GCSCheckBlobUpdateTimeTrigger._is_blob_updated_after"
)
async def test_gcs_blob_update_trigger_exception(mock_object_exists):
    """
    Tests the GCSCheckBlobUpdateTimeTrigger does fire if there is an exception.
    """
    mock_object_exists.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
    trigger = GCSCheckBlobUpdateTimeTrigger(
        TEST_BUCKET,
        TEST_OBJECT,
        TEST_TS_OBJECT,
        TEST_POLLING_INTERVAL,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blob_object_update_datetime, ts_object, expected_response",
    [
        (
            "2022-03-07T10:05:43.535Z",
            datetime(2022, 1, 1, 1, 1, 1),
            (True, {"status": "success", "message": "success"}),
        ),
        (
            "2022-03-07T10:05:43.535Z",
            datetime(2022, 3, 8, 1, 1, 1),
            (False, {"status": "pending", "message": "pending"}),
        ),
    ],
)
async def test_is_blob_updated_after(blob_object_update_datetime, ts_object, expected_response):
    """
    Tests to check if a particular object in Google Cloud Storage
    is found or not
    """
    hook = mock.AsyncMock(GCSHookAsync)
    storage = mock.AsyncMock(Storage)
    hook.get_storage_client.return_value = storage
    bucket = mock.AsyncMock(Bucket)
    storage.get_bucket.return_value = bucket
    bucket.get_blob.return_value.updated = blob_object_update_datetime
    trigger = GCSCheckBlobUpdateTimeTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        ts=ts_object,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    res = await trigger._is_blob_updated_after(hook, TEST_BUCKET, TEST_OBJECT, ts_object)
    assert res == expected_response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blob_object, expected_response",
    [
        (
            None,
            (True, {"status": "error", "message": "Object (TEST_OBJECT) not found in Bucket (TEST_BUCKET)"}),
        ),
    ],
)
async def test_is_blob_updated_after_with_none(blob_object, expected_response):
    """
    Tests to check if a particular object in Google Cloud Storage
    is found or not
    """
    hook = mock.AsyncMock(GCSHookAsync)
    storage = mock.AsyncMock(Storage)
    hook.get_storage_client.return_value = storage
    bucket = mock.AsyncMock(Bucket)
    storage.get_bucket.return_value = bucket
    bucket.get_blob.return_value = blob_object
    trigger = GCSCheckBlobUpdateTimeTrigger(
        bucket=TEST_BUCKET,
        object_name=TEST_OBJECT,
        ts=TEST_TS_OBJECT,
        polling_period_seconds=TEST_POLLING_INTERVAL,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        hook_params=TEST_HOOK_PARAMS,
    )
    res = await trigger._is_blob_updated_after(hook, TEST_BUCKET, TEST_OBJECT, TEST_TS_OBJECT)
    assert res == expected_response
