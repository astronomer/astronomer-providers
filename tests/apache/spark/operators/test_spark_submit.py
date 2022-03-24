from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.apache.spark.operators.spark_sumbit import (
    SparkSubmitOperatorAsync,
)
from astronomer.providers.apache.spark.triggers.spark_submit import SparkSubmitTrigger

TASK_ID = "spark_submit"
APPLICATION = "/opt/spark-3.1.3-bin-hadoop3.2/examples/src/main/python/pi.py"


@pytest.fixture()
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_spark_submit_operator_async():
    """
    Asserts that a task is deferred and a SparkSubmitTrigger will be fired
    when the SparkSubmitOperatorAsync is executed.
    """
    task = SparkSubmitOperatorAsync(
        task_id=TASK_ID,
        application=APPLICATION,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, SparkSubmitTrigger), "Trigger is not a SparkSubmitTrigger"


def test_spark_submit_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = SparkSubmitOperatorAsync(
        task_id=TASK_ID,
        application=APPLICATION,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "ERROR", "message": "test failure message"})


def test_spark_submit_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = SparkSubmitOperatorAsync(
        task_id=TASK_ID,
        application=APPLICATION,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event={"status": "FINISHED", "message": "Job completed"})
    mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)


def test_spark_submit_async_execute_complete_event_none():
    """Asserts that logging occurs as expected"""
    task = SparkSubmitOperatorAsync(
        task_id=TASK_ID,
        application=APPLICATION,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event=None)
    mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)
