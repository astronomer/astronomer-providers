from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.apache.hive.sensors.named_hive_partition import (
    NamedHivePartitionSensorAsync,
)
from astronomer.providers.apache.hive.triggers.named_hive_partition import (
    NamedHivePartitionTrigger,
)

TEST_PARTITION = ["user.user_profile/city=delhi"]
TEST_METASTORE_CONN_ID = "test_metastore_default"


@pytest.fixture()
def context():
    """Creates an empty context."""
    context = {}
    yield context


def test_named_hive_partition_sensor_async():
    """
    Asserts that a task is deferred and a NamedHivePartitionTrigger will be fired
    when the NamedHivePartitionSensorAsync is executed.
    """
    task = NamedHivePartitionSensorAsync(
        task_id="task-id",
        partition_names=TEST_PARTITION,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(
        exc.value.trigger, NamedHivePartitionTrigger
    ), "Trigger is not a NamedHivePartitionTrigger"


def test_named_hive_partition_sensor_async_exception():
    """
    Asserts that a task is deferred and a NamedHivePartitionTrigger will be fired
    when the NamedHivePartitionSensorAsync is executed.
    """
    task = NamedHivePartitionSensorAsync(
        task_id="task-id",
        partition_names=[],
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with pytest.raises(ValueError):
        task.execute(context)


def test_named_hive_partition_sensor_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = NamedHivePartitionSensorAsync(
        task_id="task-id",
        partition_names=[],
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_named_hive_partition_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = NamedHivePartitionSensorAsync(
        task_id="task-id",
        partition_names=TEST_PARTITION,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(
            context=None, event={"status": "success", "message": "Named hive partition found"}
        )
    mock_log_info.assert_called_with("Named hive partition found")


def test_named_hive_partition_sensor_async_execute_failure_no_event(context):
    """Assert that an AirflowException is raised in case of no event"""
    task = NamedHivePartitionSensorAsync(
        task_id="task-id",
        partition_names=TEST_PARTITION,
        metastore_conn_id=TEST_METASTORE_CONN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event=None)
