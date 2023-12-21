import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.emr import EmrJobFlowSensorTrigger

VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"
MAX_RETRIES = 5
POLL_INTERVAL = 5
JOB_FLOW_ID = "j-U0CTOZ0R20QG"
STEP_ID = "s-34RIO9CJERRJL"
TARGET_STATE = ["TERMINATED"]
FAILED_STATE = ["TERMINATED_WITH_ERRORS"]
NAME = "test-emr-job"
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


class TestEmrJobFlowSensorTrigger:
    TRIGGER = EmrJobFlowSensorTrigger(
        job_flow_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        target_states=TARGET_STATE,
        failed_states=FAILED_STATE,
        poll_interval=POLL_INTERVAL,
    )

    def test_emr_job_flow_sensors_trigger_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """

        classpath, kwargs = self.TRIGGER.serialize()
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
    async def test_emr_job_flow_sensors_trigger_run(self, mock_cluster_detail, mock_status):
        """Test if the task is run is in trigger successfully."""
        MOCK_RESPONSE["Cluster"]["Status"]["State"] = mock_status
        mock_cluster_detail.return_value = MOCK_RESPONSE

        task = asyncio.create_task(self.TRIGGER.run().__anext__())
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
    async def test_emr_job_flow_sensors_trigger_completed(self, mock_cluster_detail, mock_status):
        """Test if the task is run is in trigger failure status."""
        MOCK_RESPONSE["Cluster"]["Status"]["State"] = mock_status
        mock_cluster_detail.return_value = MOCK_RESPONSE

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        msg = f"Job flow currently {mock_status}"
        assert TriggerEvent({"status": "success", "message": msg}) == actual

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_cluster_details")
    async def test_emr_job_flow_sensors_trigger_failure_status(self, mock_cluster_detail):
        """Test if the task is run is in trigger failure status."""
        MOCK_FAILED_RESPONSE["Cluster"]["Status"]["State"] = "TERMINATED_WITH_ERRORS"
        mock_cluster_detail.return_value = MOCK_FAILED_RESPONSE

        generator = self.TRIGGER.run()
        actual = await generator.asend(None)
        final_message = "EMR job failed"
        error_code = "1111"
        msg = f"for code: {error_code} with message Failed"
        final_message += " " + msg
        assert TriggerEvent({"status": "error", "message": final_message}) == actual

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_cluster_details")
    async def test_emr_job_flow_sensors_trigger_exception(self, mock_cluster_detail):
        """Test emr job flow sensors trigger with exception"""
        mock_cluster_detail.side_effect = Exception("Test exception")

        task = [i async for i in self.TRIGGER.run()]
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
