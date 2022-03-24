import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.apache.spark.triggers.spark_submit import SparkSubmitTrigger

TASK_ID = "task_id"
POLLING_PERIOD_SECONDS = 1.0
APPLICATION = "/opt/spark-3.1.3-bin-hadoop3.2/examples/src/main/python/pi.py"
CONN_ID = "spark_default"


def test_gcs_blob_update_trigger_serialization():
    """
    Asserts that the GCSCheckBlobUpdateTimeTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = SparkSubmitTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        application=APPLICATION,
        conn_id=CONN_ID,
        name="test",
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.apache.spark.triggers.spark_submit.SparkSubmitTrigger"
    assert kwargs == {
        "task_id": TASK_ID,
        "polling_period_seconds": POLLING_PERIOD_SECONDS,
        "application": APPLICATION,
        "conn_id": CONN_ID,
        "name": "test",
        "conf": None,
        "files": None,
        "py_files": None,
        "archives": None,
        "driver_class_path": None,
        "jars": None,
        "java_class": None,
        "packages": None,
        "exclude_packages": None,
        "repositories": None,
        "total_executor_cores": None,
        "executor_cores": None,
        "executor_memory": None,
        "driver_memory": None,
        "keytab": None,
        "principal": None,
        "proxy_user": None,
        "num_executors": None,
        "application_args": None,
        "env_vars": None,
        "verbose": False,
        "spark_binary": None,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.spark_submit_job")
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.still_running")
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.get_driver_status")
async def test_spark_submit_trigger_run_success(mock_drive_status, mock_still_running, mock_spark_submit_job):
    """
    Test if the task is run and triggered successfully.
    :return:
    """
    mock_still_running.return_value = False
    mock_drive_status.return_value = {"status": "FINISHED", "message": "Previously ran and exited cleanly"}
    trigger = SparkSubmitTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        application=APPLICATION,
        conn_id=CONN_ID,
        name="test",
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "FINISHED", "message": "Previously ran and exited cleanly"}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.spark_submit_job")
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.still_running")
async def test_spark_submit_trigger_pending(mock_still_running, mock_spark_submit_job):
    """
    Test if the task is run and triggered successfully.
    :return:
    """
    mock_still_running.return_value = True
    trigger = SparkSubmitTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        application=APPLICATION,
        conn_id=CONN_ID,
        name="test",
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.spark_submit_job")
async def test_spark_submit_trigger_exception(mock_spark_submit_job):
    """
    Test if the task is run and triggered successfully.
    :return:
    """
    mock_spark_submit_job.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))
    trigger = SparkSubmitTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        application=APPLICATION,
        conn_id=CONN_ID,
        name="test",
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "ERROR", "message": "Test exception"}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.spark_submit_job")
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.still_running")
@mock.patch("astronomer.providers.apache.spark.hooks.spark_submit.SparkSubmitHookAsync.get_driver_status")
async def test_spark_submit_trigger__exception(mock_drive_status, mock_still_running, mock_spark_submit):
    """
    Test if the task is run and triggered successfully.
    :return:
    """
    mock_still_running.return_value = False
    mock_drive_status.return_value = None
    trigger = SparkSubmitTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        application=APPLICATION,
        conn_id=CONN_ID,
        name="test",
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    error_message = f"{TASK_ID} failed with terminal"
    assert TriggerEvent({"status": "ERROR", "message": error_message}) in task
