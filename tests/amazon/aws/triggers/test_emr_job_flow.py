import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.emr_job_flow import (
    EmrJobFlowSensorTrigger,
)

JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"
MAX_RETRIES = 5
POLL_INTERVAL = 5
TARGET_STATE = ["TERMINATED"]
FAILED_STATE = ["TERMINATED_WITH_ERRORS"]
MOCK_RESPONSE = {
    "Cluster": {
        "Id": "j-336EWEPYOZKOD",
        "Name": "PiCalc",
        "Status": {"State": "RUNNING", "StateChangeReason": {"Message": "Running step"}},
    },
    "ResponseMetadata": {
        "RequestId": "7256b5b8-f298-4c3e-868c-7bdf9983a4a3",
        "HTTPStatusCode": 200,
        "HTTPHeaders": {
            "x-amzn-requestid": "7256b5b8-f298-4c3e-868c-7bdf9983a4a3",
            "content-type": "application/x-amz-json-1.1",
            "content-length": "1055",
            "date": "Mon, 04 Apr 2022 13:11:54 GMT",
        },
        "RetryAttempts": 0,
    },
}
MOCK_FAILED_RESPONSE = {
    "Cluster": {
        "Id": "j-336EWEPYOZKOD",
        "Name": "PiCalc",
        "Status": {
            "State": "TERMINATED_WITH_ERRORS",
            "StateChangeReason": {"Message": "Failed", "Code": "1111"},
        },
    }
}


def test_emr_job_flow_sensors_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = EmrJobFlowSensorTrigger(
        job_flow_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        target_states=TARGET_STATE,
        failed_states=FAILED_STATE,
        poll_interval=POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.emr.EmrJobFlowSensorTrigger"
    assert kwargs == {
        "job_flow_id": JOB_ID,
        "aws_conn_id": AWS_CONN_ID,
        "target_states": TARGET_STATE,
        "failed_states": FAILED_STATE,
        "poll_interval": POLL_INTERVAL,
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
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_cluster_details")
async def test_emr_job_flow_sensors_trigger_run(mock_cluster_detail, mock_status):
    """
    Test if the task is run is in trigger successfully.
    :return:
    """
    MOCK_RESPONSE["Cluster"]["Status"]["State"] = mock_status
    mock_cluster_detail.return_value = MOCK_RESPONSE
    trigger = EmrJobFlowSensorTrigger(
        job_flow_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        target_states=TARGET_STATE,
        failed_states=FAILED_STATE,
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
    ["TERMINATED"],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_cluster_details")
async def test_emr_job_flow_sensors_trigger_completed(mock_cluster_detail, mock_status):
    """
    Test if the task is run is in trigger failure status.
    :return:
    """
    MOCK_RESPONSE["Cluster"]["Status"]["State"] = mock_status
    mock_cluster_detail.return_value = MOCK_RESPONSE
    trigger = EmrJobFlowSensorTrigger(
        job_flow_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        target_states=TARGET_STATE,
        failed_states=FAILED_STATE,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    msg = f"Job flow currently {mock_status}"
    assert TriggerEvent({"status": "success", "message": msg}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_cluster_details")
async def test_emr_job_flow_sensors_trigger_failure_status(mock_cluster_detail):
    """
    Test if the task is run is in trigger failure status.
    :return:
    """
    MOCK_FAILED_RESPONSE["Cluster"]["Status"]["State"] = "TERMINATED_WITH_ERRORS"
    print("MOCK_FAILED_RESPONSE ", MOCK_FAILED_RESPONSE)
    mock_cluster_detail.return_value = MOCK_FAILED_RESPONSE
    trigger = EmrJobFlowSensorTrigger(
        job_flow_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        target_states=TARGET_STATE,
        failed_states=FAILED_STATE,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    final_message = "EMR job failed"
    error_code = "1111"
    msg = f"for code: { error_code } with message Failed"
    final_message += " " + msg
    print("final_message ", final_message)
    print("task ", task)
    assert TriggerEvent({"status": "error", "message": final_message}) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_cluster_details")
async def test_emr_job_flow_sensors_trigger_exception(mock_cluster_detaile):
    """
    Test emr job flow sensors trigger with exception
    """
    mock_cluster_detaile.side_effect = Exception("Test exception")
    trigger = EmrJobFlowSensorTrigger(
        job_flow_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        target_states=TARGET_STATE,
        failed_states=FAILED_STATE,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
