import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.emr import EmrContainerSensorTrigger

TASK_ID = "test_emr_container_sensor"
VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"
MAX_RETRIES = 5
POLL_INTERVAL = 5


def test_emr_container_sensors_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = EmrContainerSensorTrigger(
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        max_retries=MAX_RETRIES,
        aws_conn_id=AWS_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.emr.EmrContainerSensorTrigger"
    assert kwargs == {
        "virtual_cluster_id": VIRTUAL_CLUSTER_ID,
        "job_id": JOB_ID,
        "max_retries": MAX_RETRIES,
        "poll_interval": POLL_INTERVAL,
        "aws_conn_id": AWS_CONN_ID,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_status",
    [
        "PENDING",
        "SUBMITTED",
        "RUNNING",
        None,
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.check_job_status")
async def test_emr_container_sensors_trigger_run(mock_query_status, mock_status):
    """
    Test if the task is run is in trigger successfully.
    :return:
    """
    mock_query_status.return_value = mock_status
    trigger = EmrContainerSensorTrigger(
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        max_retries=MAX_RETRIES,
        aws_conn_id=AWS_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_status",
    ["COMPLETED"],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.check_job_status")
async def test_emr_container_sensors_trigger_completed(mock_query_status, mock_status):
    """
    Test if the task is run is in trigger failure status.
    :return:
    """
    mock_query_status.return_value = mock_status
    trigger = EmrContainerSensorTrigger(
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        max_retries=MAX_RETRIES,
        aws_conn_id=AWS_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    msg = "EMR Containers sensors completed"
    assert TriggerEvent({"status": "success", "message": msg}) in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_status",
    ["FAILED", "CANCELLED", "CANCEL_PENDING"],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.check_job_status")
async def test_emr_container_sensors_trigger_failure_status(mock_query_status, mock_status):
    """
    Test if the task is run is in trigger failure status.
    :return:
    """
    mock_query_status.return_value = mock_status
    trigger = EmrContainerSensorTrigger(
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        max_retries=MAX_RETRIES,
        aws_conn_id=AWS_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    msg = f"EMR Containers sensor failed {mock_status}"
    assert TriggerEvent({"status": "error", "message": msg}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.check_job_status")
async def test_emr_container_sensors_trigger_exception(mock_query_status):
    """
    Test EMR container sensors with raise exception
    """
    mock_query_status.side_effect = Exception("Test exception")
    trigger = EmrContainerSensorTrigger(
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        job_id=JOB_ID,
        max_retries=MAX_RETRIES,
        aws_conn_id=AWS_CONN_ID,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
