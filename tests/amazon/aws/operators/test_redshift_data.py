from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

from astronomer.providers.amazon.aws.operators.redshift_data import (
    RedshiftDataOperatorAsync,
)


class TestRedshiftDataOperatorAsync:
    def test_init(self):
        task = RedshiftDataOperatorAsync(
            task_id="fetch_data",
            sql="select * from any",
            database="TEST_DATABASE",
        )
        assert isinstance(task, RedshiftDataOperator)
        assert task.deferrable is True
