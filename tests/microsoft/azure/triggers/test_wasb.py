import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.microsoft.azure.triggers.wasb import (
    WasbBlobSensorTrigger,
    WasbPrefixSensorTrigger,
)

TEST_DATA_STORAGE_BLOB_NAME = "test_blob_providers_team.txt"
TEST_DATA_STORAGE_CONTAINER_NAME = "test-container-providers-team"
TEST_DATA_STORAGE_BLOB_PREFIX = TEST_DATA_STORAGE_BLOB_NAME[:10]

TEST_WASB_CONN_ID = "wasb_default"
POLL_INTERVAL = 5.0


def test_wasb_blob_sensor_trigger_serialization():
    """
    Asserts that the WasbBlobSensorTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = WasbBlobSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.microsoft.azure.triggers.wasb.WasbBlobSensorTrigger"
    assert kwargs == {
        "container_name": TEST_DATA_STORAGE_CONTAINER_NAME,
        "blob_name": TEST_DATA_STORAGE_BLOB_NAME,
        "wasb_conn_id": TEST_WASB_CONN_ID,
        "poll_interval": POLL_INTERVAL,
        "public_read": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "blob_exists",
    [
        True,
        False,
    ],
)
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_blob")
async def test_wasb_blob_sensor_trigger_running(mock_check_for_blob, blob_exists):
    """
    Test if the task is run in trigger successfully.
    """
    mock_check_for_blob.return_value = blob_exists
    trigger = WasbBlobSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    task = asyncio.create_task(trigger.run().__anext__())

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_blob")
async def test_wasb_blob_sensor_trigger_success(mock_check_for_blob):
    """Tests the success state for that the WasbBlobSensorTrigger."""
    mock_check_for_blob.return_value = True
    trigger = WasbBlobSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    asyncio.get_event_loop().stop()

    message = f"Blob {TEST_DATA_STORAGE_BLOB_NAME} found in container {TEST_DATA_STORAGE_CONTAINER_NAME}."
    assert task.result() == TriggerEvent({"status": "success", "message": message})


@pytest.mark.asyncio
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_blob")
async def test_wasb_blob_sensor_trigger_waiting_for_blob(mock_check_for_blob):
    """Tests the WasbBlobSensorTrigger sleeps waiting for the blob to arrive."""
    mock_check_for_blob.side_effect = [False, True]

    trigger = WasbBlobSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )

    with mock.patch.object(trigger.log, "info") as mock_log_info:
        task = asyncio.create_task(trigger.run().__anext__())

    await asyncio.sleep(POLL_INTERVAL + 0.5)

    if not task.done():
        message = (
            f"Blob {TEST_DATA_STORAGE_BLOB_NAME} not available yet in container {TEST_DATA_STORAGE_CONTAINER_NAME}."
            f" Sleeping for {POLL_INTERVAL} seconds"
        )
        mock_log_info.assert_called_once_with(message)
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_blob")
async def test_wasb_blob_sensor_trigger_trigger_exception(mock_check_for_blob):
    """Tests the WasbBlobSensorTrigger yields an error event if there is an exception."""
    mock_check_for_blob.side_effect = Exception("Test exception")

    trigger = WasbBlobSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


def test_wasb_prefix_sensor_trigger_serialization():
    """
    Asserts that the WasbPrefixSensorTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = WasbPrefixSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.microsoft.azure.triggers.wasb.WasbPrefixSensorTrigger"
    assert kwargs == {
        "container_name": TEST_DATA_STORAGE_CONTAINER_NAME,
        "prefix": TEST_DATA_STORAGE_BLOB_PREFIX,
        "include": None,
        "delimiter": "/",
        "wasb_conn_id": TEST_WASB_CONN_ID,
        "poll_interval": POLL_INTERVAL,
        "public_read": False,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prefix_exists",
    [
        True,
        False,
    ],
)
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_prefix")
async def test_wasb_prefix_sensor_trigger_running(mock_check_for_prefix, prefix_exists):
    """
    Test if the task is run in trigger successfully.
    """
    mock_check_for_prefix.return_value = prefix_exists
    trigger = WasbPrefixSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    task = asyncio.create_task(trigger.run().__anext__())

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_prefix")
async def test_wasb_prefix_sensor_trigger_success(mock_check_for_prefix):
    """Tests the success state for that the WasbPrefixSensorTrigger."""
    mock_check_for_prefix.return_value = True
    trigger = WasbPrefixSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    asyncio.get_event_loop().stop()

    message = f"Prefix {TEST_DATA_STORAGE_BLOB_PREFIX} found in container {TEST_DATA_STORAGE_CONTAINER_NAME}."
    assert task.result() == TriggerEvent({"status": "success", "message": message})


@pytest.mark.asyncio
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_prefix")
async def test_wasb_prefix_sensor_trigger_waiting_for_blob(mock_check_for_prefix):
    """Tests the WasbPrefixSensorTrigger sleeps waiting for the blob to arrive."""
    mock_check_for_prefix.side_effect = [False, True]

    trigger = WasbPrefixSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )

    with mock.patch.object(trigger.log, "info") as mock_log_info:
        task = asyncio.create_task(trigger.run().__anext__())

    await asyncio.sleep(POLL_INTERVAL + 0.5)

    if not task.done():
        message = (
            f"Prefix {TEST_DATA_STORAGE_BLOB_PREFIX} not available yet in container "
            f"{TEST_DATA_STORAGE_CONTAINER_NAME}. Sleeping for {POLL_INTERVAL} seconds"
        )
        mock_log_info.assert_called_once_with(message)
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.microsoft.azure.hooks.wasb.WasbHookAsync.check_for_prefix")
async def test_wasb_prefix_sensor_trigger_trigger_exception(mock_check_for_prefix):
    """Tests the WasbPrefixSensorTrigger yields an error event if there is an exception."""
    mock_check_for_prefix.side_effect = Exception("Test exception")

    trigger = WasbPrefixSensorTrigger(
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        wasb_conn_id=TEST_WASB_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
