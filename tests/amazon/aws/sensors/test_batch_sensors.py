from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.sensors.batch import BatchSensorAsync
from astronomer.providers.amazon.aws.triggers.batch import BatchSensorTrigger

JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
AWS_CONN_ID = "airflow_test"
REGION_NAME = "eu-west-1"


@pytest.fixture
def context():
    """Creates an empty context."""
    context = {}
    yield context


def test_batch_sensor_async():
    """
    Asserts that a task is deferred and a BatchSensorTrigger will be fired
    when the BatchSensorAsync is executed.
    """
    task = BatchSensorAsync(
        task_id="task",
        job_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION_NAME,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, BatchSensorTrigger), "Trigger is not a BatchSensorTrigger"


def test_batch_sensor_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = BatchSensorAsync(
        task_id="task",
        job_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION_NAME,
    )
    with pytest.raises(AirflowException) as exc_info:
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

    assert str(exc_info.value) == "test failure message"


@pytest.mark.parametrize(
    "event",
    [{"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"}],
)
def test_batch_sensor_async_execute_complete(caplog, event):
    """Tests that execute_complete method returns None and that it prints expected log"""
    task = BatchSensorAsync(
        task_id="task",
        job_id=JOB_ID,
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION_NAME,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        assert task.execute_complete(context=None, event=event) is None

    mock_log_info.assert_called_with(event["message"])
