from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.emr import EmrContainerHookAsync

VIRTUAL_CLUSTER_ID = "test_cluster_1"
JOB_ID = "jobid-12122"
AWS_CONN_ID = "aws_default"


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
