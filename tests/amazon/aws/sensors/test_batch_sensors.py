from airflow.providers.amazon.aws.sensors.batch import BatchSensor

from astronomer.providers.amazon.aws.sensors.batch import BatchSensorAsync

MODULE = "astronomer.providers.amazon.aws.sensors.batch"


class TestBatchSensorAsync:
    def test_init(self):
        task = BatchSensorAsync(
            task_id="task",
            job_id="8ba9d676-4108-4474-9dca-8bbac1da9b19",
            aws_conn_id="airflow_test",
            region_name="eu-west-1",
        )
        assert isinstance(task, BatchSensor)
        assert task.deferrable is True
