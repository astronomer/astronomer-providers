import os
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.emr import (
    EmrContainerHookAsync,
    EmrJobFlowHookAsync,
    EmrStepSensorHookAsync,
)

VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"
JOB_FLOW_ID = "j-T0CT8Z0C20NT"
STEP_ID = "s-34RJO0CKERRPL"
JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::012345678912:role/emr_eks_default_role")
JOB_NAME = "test-emr-job"


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


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_client_async")
async def test_emr_job_flow_cluster_status(mock_client):
    """Test get_cluster_details async hook function to get the details of the cluster and job running in emr cluster
    using Aiobotocore lib"""
    mock_result = {
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
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_cluster.return_value = mock_result
    hook = EmrJobFlowHookAsync(aws_conn_id=AWS_CONN_ID)
    result = await hook.get_cluster_details(job_flow_id=JOB_ID)
    assert result == mock_result


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrJobFlowHookAsync.get_client_async")
async def test_emr_job_flow_get_cluster_details_exception(mock_client):
    """Test check_job_status async hook function to get the status of the job running in emr job flow sensor
    using Aiobotocore lib"""
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_cluster.side_effect = ClientError(
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
        operation_name="emr",
    )
    hook = EmrJobFlowHookAsync(aws_conn_id=AWS_CONN_ID)
    with pytest.raises(ClientError):
        await hook.get_cluster_details(job_flow_id=JOB_ID)


def test_job_flow_state_from_response():
    """Assert state_from_response function response"""
    response = {
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
    expected_state = EmrJobFlowHookAsync.state_from_response(response)
    assert expected_state == "RUNNING"


def test_job_flow_failure_message_from_response():
    """Assert failure_message_from_response function response"""
    response = {
        "Cluster": {
            "Id": "j-336EWEPYOZKOD",
            "Name": "PiCalc",
            "Status": {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {"Message": "Failed", "Code": 500},
            },
        },
        "ResponseMetadata": {
            "RequestId": "7256b5b8-f298-4c3e-868c-7bdf9983a4a3",
            "HTTPStatusCode": 500,
            "HTTPHeaders": {
                "x-amzn-requestid": "7256b5b8-f298-4c3e-868c-7bdf9983a4a3",
                "content-type": "application/x-amz-json-1.1",
                "content-length": "1055",
                "date": "Mon, 04 Apr 2022 13:11:54 GMT",
            },
            "RetryAttempts": 0,
        },
    }
    expected_error_message = EmrJobFlowHookAsync.failure_message_from_response(response)
    assert expected_error_message == "for code: 500 with message Failed"


def test_job_flow_failure_message_from_response_without_state_change():
    """Assert failure_message_from_response function response"""
    response = {
        "Cluster": {"Id": "j-336EWEPYOZKOD", "Name": "PiCalc", "Status": {"State": "TERMINATED_WITH_ERRORS"}}
    }
    expected_error_message = EmrJobFlowHookAsync.failure_message_from_response(response)
    assert expected_error_message is None


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.get_client_async")
async def test_get_job_failure_reason_success(mock_client):
    """Assert get_job_failure_reason return failure reason as string"""
    mock_client.return_value.__aenter__.return_value.describe_job_run.return_value = {
        "jobRun": {
            "failureReason": "Unknown",
            "stateDetails": "TERMINATED",
        }
    }
    hook = EmrContainerHookAsync(virtual_cluster_id=VIRTUAL_CLUSTER_ID, aws_conn_id=AWS_CONN_ID)
    reason = await hook.get_job_failure_reason(JOB_ID)
    expected = "Unknown - TERMINATED"
    assert reason == expected


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.get_client_async")
async def test_get_job_failure_reason_key_error(mock_client):
    """Assert get_job_failure_reason return None if failure reason not found"""
    mock_client.return_value.__aenter__.return_value.describe_job_run.return_value = {
        "jobRun": {
            "failureReason": "Unknown",
        }
    }
    hook = EmrContainerHookAsync(virtual_cluster_id=VIRTUAL_CLUSTER_ID, aws_conn_id=AWS_CONN_ID)
    reason = await hook.get_job_failure_reason(JOB_ID)
    assert reason is None


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.emr.EmrContainerHookAsync.get_client_async")
async def test_get_job_failure_reason_exception(mock_client):
    """Assert get_job_failure_reason return None on ClientError"""
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
        operation_name="emr",
    )
    hook = EmrContainerHookAsync(virtual_cluster_id=VIRTUAL_CLUSTER_ID, aws_conn_id=AWS_CONN_ID)
    reason = await hook.get_job_failure_reason(JOB_ID)
    assert reason is None
