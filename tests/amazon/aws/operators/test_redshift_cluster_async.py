from unittest import mock

from astronomer_operators.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)


def test_init_resume_operators():
    redshift_operator = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    assert redshift_operator.task_id == "task_test"
    assert redshift_operator.cluster_identifier == "test_cluster"
    assert redshift_operator.aws_conn_id == "aws_conn_test"


@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_resume_cluster_async_in_paused_state(mock_client, mock_cluster_status):
    mock_cluster_status.return_value = "paused"
    redshift_operator = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    redshift_operator.execute(None)
    mock_client.return_value.resume_cluster.assert_called_once_with(ClusterIdentifier="test_cluster")


@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_resume_cluster_async_in_available_state(mock_client, mock_cluster_status):
    mock_cluster_status.return_value = "available"
    redshift_operator = RedshiftResumeClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    redshift_operator.execute(None)
    mock_client.return_value.resume_cluster.assert_not_called()


def test_init_pause_operators():
    redshift_operator = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    assert redshift_operator.task_id == "task_test"
    assert redshift_operator.cluster_identifier == "test_cluster"
    assert redshift_operator.aws_conn_id == "aws_conn_test"


@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_pause_cluster_is_called_when_cluster_is_available(mock_client, mock_cluster_status):
    mock_cluster_status.return_value = "available"
    redshift_operator = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    redshift_operator.execute(None)
    mock_client.return_value.pause_cluster.assert_called_once_with(ClusterIdentifier="test_cluster")


@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.cluster_status")
@mock.patch("astronomer_operators.amazon.aws.hooks.redshift_cluster.RedshiftHookAsync.get_client_async")
def test_pause_cluster_not_called_when_cluster_is_not_available(mock_client, mock_cluster_status):
    mock_cluster_status.return_value = "paused"
    redshift_operator = RedshiftPauseClusterOperatorAsync(
        task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
    )
    redshift_operator.execute(None)
    mock_client.return_value.pause_cluster.assert_not_called()
