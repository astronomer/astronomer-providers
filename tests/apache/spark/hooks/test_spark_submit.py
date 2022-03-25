import asyncio
from unittest import mock

import asynctest
import pytest
from aiohttp import streams
from airflow import AirflowException

from astronomer.providers.apache.spark.hooks.spark_submit import SparkSubmitHookAsync

APPLICATION = "/opt/spark-3.1.3-bin-hadoop3.2/examples/src/main/python/pi.py"
DATA: bytes = b"line1"


@pytest.mark.asyncio
@asynctest.mock.patch("asyncio.create_subprocess_shell")
@mock.patch(
    "astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync._resolve_should_track_driver_status"
)
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync._process_submit_log")
async def test_spark_submit_job(
    mock_process_submit_log, mock_should_track_driver_status, mock_create_subprocess_shell
):
    """
    Test spark_submit_job async with mocking create_subprocess_shell and
     _resolve_should_track_driver_status in working state
    """
    process_mock = mock.Mock()
    future = asynctest.asyncio.Future()
    future.set_result(
        (
            b"""a.txt
            b.txt
            c.txt""",
            b"",
        )
    )
    attrs = {"communicate.return_value": future}
    process_mock.configure_mock(**attrs)
    mock_create_subprocess_shell.return_value = process_mock
    mock_create_subprocess_shell.return_value.returncode = None
    mock_should_track_driver_status.return_value = False
    hook = SparkSubmitHookAsync(env_vars={})
    await hook.spark_submit_job(APPLICATION)
    hook._driver_status = "FINISHED"
    mock_create_subprocess_shell.assert_called()


@pytest.mark.asyncio
@asynctest.mock.patch("asyncio.create_subprocess_shell")
@mock.patch(
    "astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync._resolve_should_track_driver_status"
)
async def test_spark_submit_job_exception(mock_should_track_driver_status, mock_create_subprocess_shell):
    """
    Test spark_submit_job async with mocking create_subprocess_shell and
     _resolve_should_track_driver_status in exception state
    """
    process_mock = mock.Mock()
    future = asynctest.asyncio.Future()
    future.set_result(
        (
            b"""a.txt
            b.txt
            c.txt""",
            b"",
        )
    )
    attrs = {"communicate.return_value": future}
    process_mock.configure_mock(**attrs)
    mock_create_subprocess_shell.return_value = process_mock
    mock_create_subprocess_shell.return_value.returncode = 1
    mock_should_track_driver_status.return_value = False
    hook = SparkSubmitHookAsync()
    with pytest.raises(AirflowException):
        await hook.spark_submit_job(APPLICATION)


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.asyncio.create_subprocess_shell")
async def test_process_submit_log(mock_create_subprocess_shell):
    """Test _process_submit_log by mocking the asyncio stream and log the info"""
    loop = asyncio.get_event_loop()
    protocol = mock.Mock(_reading_paused=False)
    stream = streams.StreamReader(protocol, 2**16, loop=loop)
    stream.feed_data(DATA)
    stream.feed_eof()
    hook = SparkSubmitHookAsync()
    with mock.patch.object(hook.log, "info") as mock_log_info:
        await hook._process_submit_log(stream)
    mock_log_info.assert_called_with(DATA.decode())


def test_track_driver_status():
    """Test the track_driver_status with mocking the _driver_status assert the result"""
    hook = SparkSubmitHookAsync()
    hook._should_track_driver_status = False
    hook._driver_status = "FINISHED"
    result = hook.track_driver_status()
    assert result == "FINISHED"


def test_track_driver_status_exception():
    """Test the track_driver_status with mocking For AirflowException"""
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
    """Test still_running function to check whether the spark submit job is completed"""
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
    """Test get_driver_status mock driver status and return the dict value response"""
    hook = SparkSubmitHookAsync()
    hook._driver_status = mock_driver_statue
    result = hook.get_driver_status()
    assert result == {"status": mock_driver_statue, "message": msg}
