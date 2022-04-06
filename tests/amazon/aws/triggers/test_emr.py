import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.emr import (
    EmrContainerSensorTrigger,
    EmrStepSensorTrigger,
)

TASK_ID = "test_emr_container_sensor"
VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"
MAX_RETRIES = 5
POLL_INTERVAL = 5
JOB_FLOW_ID = "j-U0CTOZ0R20QG"
STEP_ID = "s-34RIO9CJERRJL"


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


def test_emr_step_sensor_serialization():
    """Asserts that the EmrStepSensorTrigger correctly serializes its arguments and classpath."""
    trigger = EmrStepSensorTrigger(
        job_flow_id=JOB_FLOW_ID, step_id=STEP_ID, aws_conn_id=AWS_CONN_ID, poke_interval=60
    )

    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.emr.EmrStepSensorTrigger"
    assert kwargs == {
        "job_flow_id": JOB_FLOW_ID,
        "step_id": STEP_ID,
        "aws_conn_id": AWS_CONN_ID,
        "poke_interval": 60,
        "target_states": ["COMPLETED"],
        "failed_states": ["CANCELLED", "FAILED", "INTERRUPTED"],
    }


def _emr_describe_step_response(state):
    """Return dummy response dict for emr_describe_step method"""
    return {
        "Step": {
            "Id": STEP_ID,
            "Name": "PiCir",
            "ActionOnFailure": "TERMINATE_JOB_FLOW",
            "Status": {
                "State": state,
                "FailureDetails": {"Reason": "Unknown Error", "Message": "", "LogFile": ""},
            },
        }
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrStepSensorHookAsync.emr_describe_step")
async def test_emr_step_sensor_trigger_run_success(emr_describe_step):
    """Assert EmrStepSensorTrigger run method success"""
    emr_describe_step.return_value = _emr_describe_step_response("COMPLETED")
    trigger = EmrStepSensorTrigger(
        job_flow_id=JOB_FLOW_ID, step_id=STEP_ID, aws_conn_id=AWS_CONN_ID, poke_interval=60
    )
    task = [i async for i in trigger.run()]
    response = TriggerEvent({"status": "success", "message": "Job flow currently COMPLETED"})
    assert len(task) == 1
    assert response in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_response",
    [
        _emr_describe_step_response("PENDING"),
        _emr_describe_step_response("CANCEL_PENDING"),
        _emr_describe_step_response("RUNNING"),
        _emr_describe_step_response(None),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrStepSensorHookAsync.emr_describe_step")
async def test_emr_step_sensor_trigger_run_pending(emr_describe_step, mock_response):
    """Assert run method of EmrStepSensorHookAsync sleep"""
    emr_describe_step.return_value = mock_response
    trigger = EmrStepSensorTrigger(
        job_flow_id=JOB_FLOW_ID, step_id=STEP_ID, aws_conn_id=AWS_CONN_ID, poke_interval=5
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_response",
    [
        _emr_describe_step_response("CANCELLED"),
        _emr_describe_step_response("FAILED"),
        _emr_describe_step_response("INTERRUPTED"),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrStepSensorHookAsync.emr_describe_step")
async def test_emr_step_sensor_trigger_run_fail(emr_describe_step, mock_response):
    """Assert run method of EmrStepSensorHookAsync fail"""
    emr_describe_step.return_value = mock_response
    trigger = EmrStepSensorTrigger(
        job_flow_id=JOB_FLOW_ID,
        step_id=STEP_ID,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=5,
        failed_states=["CANCELLED", "FAILED", "INTERRUPTED"],
    )
    task = [i async for i in trigger.run()]
    response = TriggerEvent(
        {"status": "error", "message": "for reason Unknown Error with message  and log file "}
    )
    assert len(task) == 1
    assert response in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrStepSensorHookAsync.emr_describe_step")
async def test_emr_step_sensor_trigger_run_failure(emr_describe_step):
    """Test EmrStepSensorTrigger run method fail"""
    emr_describe_step.side_effect = Exception("Test exception")
    trigger = EmrStepSensorTrigger(
        job_flow_id=JOB_FLOW_ID, step_id=STEP_ID, aws_conn_id=AWS_CONN_ID, poke_interval=60
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
