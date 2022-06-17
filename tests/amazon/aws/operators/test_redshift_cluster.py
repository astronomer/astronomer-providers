from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperatorAsync,
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
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.delete_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_delete_cluster(mock_async_client, mock_async_delete_cluster, mock_sync_cluster_status):
    """Test Delete cluster operator with available cluster state and check the trigger instance"""
    mock_sync_cluster_status.return_value = "available"
    mock_async_client.return_value.delete_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "deleting"}
    }
    mock_async_delete_cluster.return_value = {"status": "success", "cluster_state": "cluster_not_found"}

    redshift_operator = RedshiftDeleteClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with pytest.raises(TaskDeferred) as exc:
        redshift_operator.execute(context)

    assert isinstance(exc.value.trigger, RedshiftClusterTrigger), "Trigger is not a RedshiftClusterTrigger"


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.delete_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_delete_cluster_failure(mock_async_client, mock_async_delete_cluster, mock_sync_cluster_status):
    """Test Delete cluster operator with available cluster state in failure test case"""
    mock_sync_cluster_status.return_value = "available"
    mock_async_client.return_value.delete_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "deleting"}
    }
    mock_async_delete_cluster.return_value = {"status": "success", "cluster_state": "cluster_not_found"}

    redshift_operator = RedshiftDeleteClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with pytest.raises(AirflowException):
        redshift_operator.execute_complete(
            context=None, event={"status": "error", "message": "test failure message"}
        )


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.delete_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_delete_cluster_execute_complete(
    mock_async_client, mock_async_delete_cluster, mock_sync_cluster_status
):
    """
    Test Delete cluster operator execute_complete with available cluster state and return state as cluster_not_found.
    """
    mock_sync_cluster_status.return_value = "available"
    mock_async_client.return_value.delete_cluster.return_value = {
        "Cluster": {"ClusterIdentifier": "test_cluster", "ClusterStatus": "deleting"}
    }
    mock_async_delete_cluster.return_value = {"status": "success", "cluster_state": "cluster_not_found"}

    redshift_operator = RedshiftDeleteClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with mock.patch.object(redshift_operator.log, "info") as mock_log_info:
        redshift_operator.execute_complete(
            context=None, event={"status": "success", "cluster_state": "cluster_not_found"}
        )
    mock_log_info.assert_called_with("Deleted cluster successfully")


def test_delete_cluster_execute_complete_invalid_trigger_event():
    """Asserts that exception is raised when invalid event is received from triggerer"""
    task = RedshiftDeleteClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    with pytest.raises(AirflowException) as exception_info:
        task.execute_complete(context=None, event=None)

    assert exception_info.value.args[0] == "Did not receive valid event from the trigerrer"


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
def test_delete_cluster_execute_warning(mock_sync_cluster_status):
    """Test Pause cluster operator execute method with warnings message"""
    mock_sync_cluster_status.return_value = "cluster_not_found"
    redshift_operator = RedshiftDeleteClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with mock.patch.object(redshift_operator.log, "warning") as mock_log_warning:
        redshift_operator.execute(context=None)
    mock_log_warning.assert_called_with(
        "Unable to delete cluster since cluster is not found. It may have already been deleted"
    )


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.resume_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_resume_cluster(mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue):
    """Test Resume cluster operator run"""
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
    """Test Resume cluster operator Failure"""
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
    """Test Resume cluster operator execute_complete with proper return value"""
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


def test_resume_cluster_execute_complete_with_event_none():
    """Asserts that logging occurs as expected"""
    task = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event=None)
    mock_log_info.assert_called_with("%s completed successfully.", "task_test")


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.pause_cluster")
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_pause_cluster(mock_async_client, mock_async_resume_cluster, mock_sync_cluster_statue):
    """Test Pause cluster operator with available cluster state and check the trigger instance"""
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
    """Test Pause cluster operator with available cluster state in failure test case"""
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
    """Test Pause cluster operator execute_complete with available cluster state and return state as paused"""
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


def test_pause_cluster_execute_complete_none():
    """Asserts that logging occurs as expected"""
    task = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event=None)
    mock_log_info.assert_called_with("%s completed successfully.", "task_test")


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
def test_pause_cluster_execute_warning(mock_sync_cluster_statue):
    """Test Pause cluster operator execute method with warnings message"""
    mock_sync_cluster_statue.return_value = "paused"
    redshift_operator = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with mock.patch.object(redshift_operator.log, "warning") as mock_log_warning:
        redshift_operator.execute(context=None)
    mock_log_warning.assert_called_with(
        "Unable to pause cluster since cluster is currently in status: %s", "paused"
    )


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
def test_resume_cluster_execute_warning(mock_sync_cluster_statue):
    """Test Pause resume operator execute method with warnings message"""
    mock_sync_cluster_statue.return_value = "available"
    redshift_operator = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )

    with mock.patch.object(redshift_operator.log, "warning") as mock_log_warning:
        redshift_operator.execute(context=None)
    mock_log_warning.assert_called_with(
        "Unable to resume cluster since cluster is currently in status: %s", "available"
    )
