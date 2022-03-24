from unittest import mock

import pytest
from airflow import AirflowException

from astronomer.providers.apache.spark.hooks.spark_submit import SparkSubmitHookAsync


def test_track_driver_status():
    hook = SparkSubmitHookAsync()
    hook._should_track_driver_status = False
    hook._driver_status = "FINISHED"
    result = hook.track_driver_status()
    assert result == "FINISHED"


def test_track_driver_status_exception():
    hook = SparkSubmitHookAsync()
    hook._should_track_driver_status = True
    hook._driver_id = None
    with pytest.raises(AirflowException):
        hook.track_driver_status()


@pytest.mark.parametrize(
    "mock_driver_statue, mock_response",
    [("FINISHED", False), ("SUBMITTED", True)],
)
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.track_driver_status")
def test_still_running(mock_track_driver_status, mock_driver_statue, mock_response):
    mock_track_driver_status.return_value = mock_driver_statue
    hook = SparkSubmitHookAsync()
    result = hook.still_running()
    assert result == mock_response


@pytest.mark.parametrize(
    "mock_driver_statue, msg",
    [
        ("SUBMITTED", "Submitted but not yet scheduled on a worker"),
        ("RUNNING", "Has been allocated to a worker to run"),
        ("FINISHED", "Previously ran and exited cleanly"),
        ("RELAUNCHING", "Exited non-zero or due to worker failure, but has not yet started running again"),
        ("UNKNOWN", "The status of the driver is temporarily not known due to master failure recovery"),
        ("KILLED", "A user manually killed this driver"),
        ("FAILED", "The driver exited non-zero and was not supervised"),
        ("ERROR", "Unable to run or restart due to an unrecoverable error"),
    ],
)
def test_get_driver_status(mock_driver_statue, msg):
    hook = SparkSubmitHookAsync()
    hook._driver_status = mock_driver_statue
    result = hook.get_driver_status()
    assert result == {"status": mock_driver_statue, "message": msg}
