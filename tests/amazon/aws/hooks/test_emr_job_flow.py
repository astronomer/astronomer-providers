from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.emr_job_flow import EmrJobFlowHookAsync

JOB_ID = "j-336EWEPYOZKOD"
AWS_CONN_ID = "aws_default"


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


def test_state_from_response():
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


def test_failure_message_from_response():
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


def test_failure_message_from_response_without_state_change():
    """Assert failure_message_from_response function response"""
    response = {
        "Cluster": {"Id": "j-336EWEPYOZKOD", "Name": "PiCalc", "Status": {"State": "TERMINATED_WITH_ERRORS"}}
    }
    expected_error_message = EmrJobFlowHookAsync.failure_message_from_response(response)
    assert expected_error_message is None
