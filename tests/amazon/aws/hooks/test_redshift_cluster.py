from unittest import mock

import pytest

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
        ("astro-redshift-cluster-1", None, {"Clusters": []}),
        (
            "astro-redshift-cluster-1",
            {
                "status": "error",
                "message": "ClusterNotFoundFault('An error occurred "
                "(ClusterNotFound) when calling the "
                "DescribeClusters "
                "operation: Cluster test111 not found.",
            },
            "ClusterNotFoundFault('An error occurred "
            "(ClusterNotFound) when calling the "
            "DescribeClusters "
            "operation: Cluster test111 not found.",
        ),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_redshift_cluster_status(
    mock_client, mock_cluster_status, mock_cluster_identifier, cluster_state, expected_result
):
    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    mock_client.return_value.describe_clusters.return_value = expected_result
    mock_cluster_status.return_value = cluster_state
    result = await hook.cluster_status(cluster_identifier=mock_cluster_identifier)
    assert result == cluster_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_cluster_identifier, cluster_state, expected_result",
    [
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "paused"},
            {"Cluster": {"ClusterIdentifier": "astro-redshift-cluster-1", "ClusterStatus": "pausing"}},
        ),
        (
            "astro-redshift-cluster-1",
            {
                "status": "error",
                "message": "An error occurred (InvalidClusterState) when calling "
                "the PauseCluster operation: You can only pause an ACTIVE Cluster",
            },
            "An error occurred (InvalidClusterState) when calling the PauseCluster operation:"
            " You can only pause an ACTIVE Cluster",
        ),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_pause_cluster(
    mock_client, mock_cluster_status, mock_cluster_identifier, cluster_state, expected_result
):
    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    mock_client.pause_cluster.return_value = expected_result
    mock_cluster_status.return_value = cluster_state
    result = await hook.pause_cluster(cluster_identifier=mock_cluster_identifier)
    assert result == cluster_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_cluster_identifier, cluster_state, expected_result",
    [
        (
            "astro-redshift-cluster-1",
            {"status": "success", "cluster_state": "available"},
            {"Cluster": {"ClusterIdentifier": "astro-redshift-cluster-1", "ClusterStatus": "resuming"}},
        ),
        (
            "astro-redshift-cluster-1",
            {
                "status": "error",
                "message": "An error occurred (InvalidClusterState) when "
                "calling the ResumeCluster operation: You can only resume a "
                "PAUSED Cluster",
            },
            "An error occurred (InvalidClusterState) when "
            "calling the ResumeCluster operation: You can only resume a "
            "PAUSED Cluster",
        ),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
async def test_resume_cluster(
    mock_client, mock_cluster_status, mock_cluster_identifier, cluster_state, expected_result
):
    hook = RedshiftHookAsync(aws_conn_id="test_aws_connection_id")
    mock_client.resume_cluster.return_value = expected_result
    mock_cluster_status.return_value = cluster_state
    result = await hook.resume_cluster(cluster_identifier=mock_cluster_identifier)
    assert result == cluster_state
