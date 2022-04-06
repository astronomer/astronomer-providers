from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.emr import (
    EmrContainerHookAsync,
    EmrStepSensorHookAsync,
)

VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"
JOB_FLOW_ID = "j-T0CT8Z0C20NT"
STEP_ID = "s-34RJO0CKERRPL"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_expected_job_state, mock_result",
    [
        (
            "FAILED",
            {"jobRun": {"state": "FAILED"}},
        ),
        (
            "CANCELLED",
            {"jobRun": {"state": "CANCELLED"}},
        ),
        (
            "CANCEL_PENDING",
            {"jobRun": {"state": "CANCEL_PENDING"}},
        ),
        (
            "PENDING",
            {"jobRun": {"state": "PENDING"}},
        ),
        (
            "SUBMITTED",
            {"jobRun": {"state": "SUBMITTED"}},
        ),
        (
            "RUNNING",
            {"jobRun": {"state": "RUNNING"}},
        ),
        (
            None,
            {},
        ),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.get_client_async")
async def test_emr_container_cluster_status(mock_client, mock_expected_job_state, mock_result):
    """Test check_job_status async hook function to get the status of the job running in emr container
    using Aiobotocore lib"""
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_job_run.return_value = mock_result
    hook = EmrContainerHookAsync(aws_conn_id=AWS_CONN_ID, virtual_cluster_id=VIRTUAL_CLUSTER_ID)
    result = await hook.check_job_status(job_id=JOB_ID)
    assert result == mock_expected_job_state


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.get_client_async")
async def test_emr_container_cluster_exception(mock_client):
    """Test check_job_status async hook function to get the status of the job running in emr container
    using Aiobotocore lib"""
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_job_run.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeServiceException",
                "Message": "Details/context around the exception or error",
            },
            "ResponseMetadata": {
                "RequestId": "1234567890ABCDEF",
                "HostId": "host ID data will appear here as a hash",
                "HTTPStatusCode": 404,
                "HTTPHeaders": {"header metadata key/values will appear here"},
                "RetryAttempts": 0,
            },
        },
        operation_name="emr-containers",
    )
    hook = EmrContainerHookAsync(aws_conn_id=AWS_CONN_ID, virtual_cluster_id=VIRTUAL_CLUSTER_ID)
    with pytest.raises(ClientError):
        await hook.check_job_status(job_id=JOB_ID)


def _emr_describe_step_response(state: str = "", reason: str = "", message: str = "", logfile: str = ""):
    """Return a dummy response for emr_describe_step method"""
    return {
        "Step": {
            "Id": STEP_ID,
            "Name": "PiCir",
            "ActionOnFailure": "TERMINATE_JOB_FLOW",
            "Status": {
                "State": state,
                "FailureDetails": {
                    "Reason": reason,
                    "Message": message,
                    "LogFile": logfile,
                },
            },
        }
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrStepSensorHookAsync.get_client_async")
async def test_emr_describe_step_success(mock_client):
    """Assert check_job_status method return Steps"""
    expected = _emr_describe_step_response()
    mock_client.return_value.__aenter__.return_value.describe_step.return_value = expected

    hook = EmrStepSensorHookAsync(aws_conn_id=AWS_CONN_ID, job_flow_id=JOB_FLOW_ID, step_id=STEP_ID)
    response = await hook.emr_describe_step()
    assert response == expected


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrStepSensorHookAsync.get_client_async")
async def test_emr_describe_step_failed(mock_client):
    """Assert emr_describe_step method throw exception"""
    mock_client.return_value.__aenter__.return_value.describe_step.side_effect = ClientError(
        error_response={}, operation_name=""
    )
    hook = EmrStepSensorHookAsync(aws_conn_id=AWS_CONN_ID, job_flow_id=JOB_FLOW_ID, step_id=STEP_ID)
    with pytest.raises(ClientError):
        await hook.emr_describe_step()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    [
        _emr_describe_step_response("PENDING"),
        _emr_describe_step_response("CANCEL_PENDING"),
        _emr_describe_step_response("RUNNING"),
        _emr_describe_step_response("COMPLETED"),
        _emr_describe_step_response("CANCELLED"),
        _emr_describe_step_response("FAILED"),
        _emr_describe_step_response("INTERRUPTED"),
    ],
)
def test_state_from_response(state):
    """Assert state_from_response function response"""
    response = _emr_describe_step_response(state=state)
    expected_state = EmrStepSensorHookAsync.state_from_response(response)
    assert expected_state == state


def test_failure_message_from_response():
    """Assert failure_message_from_response function response"""
    state = "FAILED"
    reason = "Unknown Error"
    message = "failed with Unknown Error"
    logfile = "/usr/logs/emr.log"
    response = _emr_describe_step_response(state=state, reason=reason, message=message, logfile=logfile)
    error_message = EmrStepSensorHookAsync.failure_message_from_response(response)

    assert error_message == f"for reason {reason} with message {message} and log file {logfile}"

    # assert for FailureDetails missing in response
    response["Step"]["Status"] = {"State": "FAILED"}
    error_message = EmrStepSensorHookAsync.failure_message_from_response(response)
    assert error_message is None
