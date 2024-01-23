from airflow.providers.amazon.aws.operators.batch import BatchOperator

from astronomer.providers.amazon.aws.operators.batch import BatchOperatorAsync


class TestBatchOperatorAsync:
    def test_init(self):
        task = BatchOperatorAsync(
            task_id="task",
            job_name="51455483-c62c-48ac-9b88-53a6a725baa3",
            job_queue="queue",
            job_definition="hello-world",
            max_retries=2,
            status_retries=3,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            tags={},
        )
        assert isinstance(task, BatchOperator)
        assert task.deferrable is True
