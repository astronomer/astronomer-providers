from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensorAsync,
)
from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryTableExistenceTrigger,
)

PROJECT_ID = "test-astronomer-airflow-providers"
DATASET_NAME = "test-astro_dataset"
TABLE_NAME = "test-partitioned_table"


@pytest.fixture()
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_big_query_table_existence_sensor_async():
    """
    Asserts that a task is deferred and a BigQueryTableExistenceTrigger will be fired
    when the BigQueryTableExistenceSensorAsync is executed.
    """
    task = BigQueryTableExistenceSensorAsync(
        task_id="check_table_exists",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(
        exc.value.trigger, BigQueryTableExistenceTrigger
    ), "Trigger is not a BigQueryTableExistenceTrigger"


def test_big_query_table_existence_sensor_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = BigQueryTableExistenceSensorAsync(
        task_id="task-id",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_big_query_table_existence_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = BigQueryTableExistenceSensorAsync(
        task_id="task-id",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    table_uri = f"{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}"
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
    mock_log_info.assert_called_with("Sensor checks existence of table: %s", table_uri)


def test_redshift_sensor_async_execute_complete_event_none():
    """Asserts that logging occurs as expected"""
    task = BigQueryTableExistenceSensorAsync(
        task_id="task-id",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event=None)
