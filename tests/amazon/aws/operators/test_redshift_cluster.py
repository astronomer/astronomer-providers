from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperatorAsync,
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)


class TestRedshiftDeleteClusterOperatorAsync:
    def test_init(self):
        task = RedshiftDeleteClusterOperatorAsync(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        assert isinstance(task, RedshiftDeleteClusterOperator)
        assert task.deferrable is True


class TestRedshiftPauseClusterOperatorAsync:
    def test_init(self):
        task = RedshiftPauseClusterOperatorAsync(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )

        assert isinstance(task, RedshiftPauseClusterOperator)
        assert task.deferrable is True


class TestRedshiftResumeClusterOperatorAsync:
    def test_init(self):
        task = RedshiftResumeClusterOperatorAsync(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )

        assert isinstance(task, RedshiftResumeClusterOperator)
        assert task.deferrable is True
