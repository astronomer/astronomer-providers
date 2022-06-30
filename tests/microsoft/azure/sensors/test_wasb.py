from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensorAsync,
    WasbPrefixSensorAsync,
)
from astronomer.providers.microsoft.azure.triggers.wasb import (
    WasbBlobSensorTrigger,
    WasbPrefixSensorTrigger,
)

TEST_DATA_STORAGE_BLOB_NAME = "test_blob_providers_team.txt"
TEST_DATA_STORAGE_CONTAINER_NAME = "test-container-providers-team"
TEST_DATA_STORAGE_BLOB_PREFIX = TEST_DATA_STORAGE_BLOB_NAME[:10]


@pytest.fixture
def context():
    """Creates an empty context."""
    context = {}
    yield context


def test_wasb_blob_sensor_async(context):
    """Assert execute method defer for wasb blob sensor"""
    task = WasbBlobSensorAsync(
        task_id="wasb_blob_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, WasbBlobSensorTrigger), "Trigger is not a WasbBlobSensorTrigger"


@pytest.mark.parametrize(
    "event",
    [None, {"status": "success", "message": "Job completed"}],
)
def test_wasb_blob_sensor_execute_complete_success(event):
    """Assert execute_complete log success message when trigger fire with target status."""
    task = WasbBlobSensorAsync(
        task_id="wasb_blob_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
    )

    if not event:
        with pytest.raises(AirflowException) as exception_info:
            task.execute_complete(context=None, event=None)
        assert exception_info.value.args[0] == "Did not receive valid event from the triggerer"
    else:
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context={}, event=event)
        mock_log_info.assert_called_with(event["message"])


def test_wasb_blob_sensor_execute_complete_failure():
    """Assert execute_complete method raises an exception when the triggerer fires an error event."""
    task = WasbBlobSensorAsync(
        task_id="wasb_blob_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context={}, event={"status": "error", "message": ""})


def test_wasb_prefix_sensor_async(context):
    """Assert execute method defer for wasb prefix sensor"""
    task = WasbPrefixSensorAsync(
        task_id="wasb_prefix_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, WasbPrefixSensorTrigger), "Trigger is not a WasbPrefixSensorTrigger"


@pytest.mark.parametrize(
    "event",
    [None, {"status": "success", "message": "Job completed"}],
)
def test_wasb_prefix_sensor_execute_complete_success(event):
    """Assert execute_complete log success message when trigger fire with target status."""
    task = WasbPrefixSensorAsync(
        task_id="wasb_prefix_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_NAME,
    )

    if not event:
        with pytest.raises(AirflowException) as exception_info:
            task.execute_complete(context=None, event=None)
        assert exception_info.value.args[0] == "Did not receive valid event from the triggerer"
    else:
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context={}, event=event)
        mock_log_info.assert_called_with(event["message"])


def test_wasb_prefix_sensor_execute_complete_failure():
    """Assert execute_complete method raises an exception when the triggerer fires an error event."""
    task = WasbPrefixSensorAsync(
        task_id="wasb_prefix_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_NAME,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context={}, event={"status": "error", "message": ""})
