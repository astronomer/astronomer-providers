from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)
from astronomer.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterTrigger,
)


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_resume_cluster(mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue):
    mock_sync_cluster_statue.return_value = "paused"
    mock_async_client.return_value.resume_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "resuming"}
    }
    mock_async_resume_cluster.return_value = {"status": "success", "cluster_state": "available"}

    redshift_operator = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with pytest.raises(TaskDeferred) as exc:
        redshift_operator.execute(context)

    assert isinstance(exc.value.trigger, RedshiftClusterTrigger), "Trigger is not a RedshiftClusterTrigger"


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_resume_cluster_failure(mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue):
    mock_sync_cluster_statue.return_value = "paused"
    mock_async_client.return_value.resume_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "resuming"}
    }
    mock_async_resume_cluster.return_value = {"status": "success", "cluster_state": "available"}

    redshift_operator = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with pytest.raises(AirflowException):
        redshift_operator.execute_complete(
            context=None, event={"status": "error", "message": "test failure message"}
        )


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_resume_cluster_execute_complete(
    mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue
):
    mock_sync_cluster_statue.return_value = "paused"
    mock_async_client.return_value.resume_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "resuming"}
    }
    mock_async_resume_cluster.return_value = {"status": "success", "cluster_state": "available"}

    redshift_operator = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with mock.patch.object(redshift_operator.log, "info") as mock_log_info:
        redshift_operator.execute_complete(
            context=None, event={"status": "success", "cluster_state": "available"}
        )
    mock_log_info.assert_called_with("Resumed cluster successfully, now its in available state")


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_pause_cluster(mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue):
    mock_sync_cluster_statue.return_value = "available"
    mock_async_client.return_value.pause_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "pausing"}
    }
    mock_async_resume_cluster.return_value = {"status": "success", "cluster_state": "paused"}

    redshift_operator = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with pytest.raises(TaskDeferred) as exc:
        redshift_operator.execute(context)

    assert isinstance(exc.value.trigger, RedshiftClusterTrigger), "Trigger is not a RedshiftClusterTrigger"


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_pause_cluster_failure(mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue):
    mock_sync_cluster_statue.return_value = "available"
    mock_async_client.return_value.pause_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "pausing"}
    }
    mock_async_resume_cluster.return_value = {"status": "success", "cluster_state": "paused"}

    redshift_operator = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with pytest.raises(AirflowException):
        redshift_operator.execute_complete(
            context=None, event={"status": "error", "message": "test failure message"}
        )


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_pause_cluster_execute_complete(
    mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue
):
    mock_sync_cluster_statue.return_value = "available"
    mock_async_client.return_value.pause_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "pausing"}
    }
    mock_async_resume_cluster.return_value = {"status": "success", "cluster_state": "paused"}

    redshift_operator = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with mock.patch.object(redshift_operator.log, "info") as mock_log_info:
        redshift_operator.execute_complete(
            context=None, event={"status": "success", "cluster_state": "paused"}
        )
    mock_log_info.assert_called_with("Paused cluster successfully")
