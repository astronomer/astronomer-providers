import asyncio
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.hooks.redshift_cluster import RedshiftHookAsync


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_cluster_identifier, cluster_state, expected_result",
    [
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "available"},
            {"Clusters": [{"ClusterStatus": "available"}]},
        ),
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "paused"},
            {"Clusters": [{"ClusterStatus": "paused"}]},
        ),
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "pausing"},
            {"Clusters": [{"ClusterStatus": "pausing"}]},
        ),
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "resuming"},
            {"Clusters": [{"ClusterStatus": "resuming"}]},
        ),
        ("astro-redshift-cluster-1", {"status": "success", "cluster_state": None}, {"Clusters": []}),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_redshift_cluster_status(mock_client, mock_cluster_identifier, cluster_state, expected_result):
    """Test cluster status async hook function to get the cluster status by calling Aiobotocore lib"""
    # mocking async context function with return_value of __aenter__
    mock_client.return_value.__aenter__.return_value.describe_clusters.return_value = expected_result
    hook = RedshiftHookAsync(
        aws_conn_id="test_aws_connection_id", client_type="redshift", resource_type="redshift"
    )
    result = await hook.cluster_status(cluster_identifier=mock_cluster_identifier)
    assert result == cluster_state


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_redshift_cluster_status_exception(mock_client):
    """Test cluster status async hook function by mocking exception"""
    mock_client.return_value.__aenter__.return_value.describe_clusters.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeServiceException",
                "Message": "Details/context around the exception or error",
            },
            "ResponseMetadata": {
                "RequestId": "1234567890ABCDEF",
                "HostId": "host ID data will appear here as a hash",
                "HTTPStatusCode": 500,
                "HTTPHeaders": {"header metadata key/values will appear here"},
                "RetryAttempts": 0,
            },
        },
        operation_name="redshift",
    )
    hook = RedshiftHookAsync(
        aws_conn_id="test_aws_connection_id", client_type="redshift", resource_type="redshift"
    )
    result = await hook.cluster_status(cluster_identifier="astro-redshift-cluster-1")
    assert result == {
        "status": "error",
        "message": "An error occurred (SomeServiceException) when calling the "
        "redshift operation: Details/context around the exception or error",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_cluster_identifier, cluster_state, expected_result",
    [
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "paused"},
            {"Cluster": {"ClusterIdentifier": "astro-redshift-cluster-1", "ClusterStatus": "pausing"}},
        ),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_pause_cluster(
    mock_client, mock_cluster_status, mock_cluster_identifier, cluster_state, expected_result
):
    """Test Pause cluster async hook function by mocking return value of pause_cluster"""
    mock_client.return_value.__aenter__.return_value.pause_cluster.return_value = expected_result
    mock_cluster_status.return_value = cluster_state

    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    task = await hook.pause_cluster(cluster_identifier=mock_cluster_identifier)

    assert task == cluster_state


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_pause_cluster_exception(mock_client):
    """Test Pause cluster async hook function with exception by mocking return value of pause_cluster"""
    mock_client.return_value.__aenter__.return_value.pause_cluster.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeServiceException",
                "Message": "Details/context around the exception or error",
            },
            "ResponseMetadata": {
                "RequestId": "1234567890ABCDEF",
                "HostId": "host ID data will appear here as a hash",
                "HTTPStatusCode": 500,
                "HTTPHeaders": {"header metadata key/values will appear here"},
                "RetryAttempts": 0,
            },
        },
        operation_name="redshift",
    )
    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    result = await hook.pause_cluster(cluster_identifier="test")
    assert result == {
        "status": "error",
        "message": "An error occurred (SomeServiceException) when calling the "
        "redshift operation: Details/context around the exception or error",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_cluster_identifier, cluster_state, expected_result",
    [
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "available"},
            {"Cluster": {"ClusterIdentifier": "astro-redshift-cluster-1", "ClusterStatus": "resuming"}},
        )
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_resume_cluster(
    mock_client, mock_cluster_status, mock_cluster_identifier, cluster_state, expected_result
):
    """Test Resume cluster async hook function by mocking return value of resume_cluster"""
    mock_client.return_value.__aenter__.return_value.resume_cluster.return_value = expected_result
    mock_cluster_status.return_value = cluster_state

    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    task = await hook.resume_cluster(cluster_identifier=mock_cluster_identifier)

    assert task == cluster_state


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_resume_cluster_exception(mock_client):
    """Test Resume cluster async hook function with exception by mocking return value of resume_cluster"""
    mock_client.return_value.__aenter__.return_value.resume_cluster.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeServiceException",
                "Message": "Details/context around the exception or error",
            },
            "ResponseMetadata": {
                "RequestId": "1234567890ABCDEF",
                "HostId": "host ID data will appear here as a hash",
                "HTTPStatusCode": 500,
                "HTTPHeaders": {"header metadata key/values will appear here"},
                "RetryAttempts": 0,
            },
        },
        operation_name="redshift",
    )
    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    result = await hook.resume_cluster(cluster_identifier="test")
    assert result == {
        "status": "error",
        "message": "An error occurred (SomeServiceException) when calling the "
        "redshift operation: Details/context around the exception or error",
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
async def test_get_cluster_status(cluster_status, mock_client):
    """Test get_cluster_status async function with success response"""
    flag = asyncio.Event()
    cluster_status.return_value = {"status": "success", "cluster_state": "available"}
    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    result = await hook.get_cluster_status("test-identifier", "available", flag)
    assert result == {"status": "success", "cluster_state": "available"}


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
async def test_get_cluster_status_exception(cluster_status):
    """Test get_cluster_status async function with exception response"""
    flag = asyncio.Event()
    cluster_status.side_effect = ClientError(
        {
            "Error": {
                "Code": "SomeServiceException",
                "Message": "Details/context around the exception or error",
            },
            "ResponseMetadata": {
                "RequestId": "1234567890ABCDEF",
                "HostId": "host ID data will appear here as a hash",
                "HTTPStatusCode": 500,
                "HTTPHeaders": {"header metadata key/values will appear here"},
                "RetryAttempts": 0,
            },
        },
        operation_name="redshift",
    )
    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    result = await hook.get_cluster_status("test-identifier", "available", flag)
    assert result == {
        "status": "error",
        "message": "An error occurred (SomeServiceException) when calling the "
        "redshift operation: Details/context around the exception or error",
    }
