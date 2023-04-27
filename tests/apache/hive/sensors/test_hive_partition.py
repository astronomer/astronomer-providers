from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.apache.hive.sensors.hive_partition import (
    HivePartitionSensorAsync,
)
from astronomer.providers.apache.hive.triggers.hive_partition import (
    HivePartitionTrigger,
)

TEST_TABLE = "test_table"
TEST_PARTITION = "state='FL'"
TEST_METASTORE_CONN_ID = "test_metastore_default"


MODULE = "astronomer.providers.apache.hive.sensors.hive_partition"


class TestHivePartitionSensorAsync:
    @mock.patch(f"{MODULE}.HivePartitionSensorAsync.defer")
    @mock.patch(
        "astronomer.providers.apache.hive.sensors.hive_partition.HiveCliHookAsync",
    )
    def test_hive_partition_sensor_async_finish_before_deferred(self, mock_hook, mock_defer, context):
        mock_hook.return_value.check_partition_exists.return_value = True
        task = HivePartitionSensorAsync(
            task_id="task-id",
            table=TEST_TABLE,
            partition=TEST_PARTITION,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        task.execute(context)

        assert not mock_defer.called

    @mock.patch(
        "astronomer.providers.apache.hive.sensors.hive_partition.HiveCliHookAsync",
    )
    def test_hive_partition_sensor_async(self, mock_hook, context):
        """
        Asserts that a task is deferred and a HivePartitionTrigger will be fired
        when the HivePartitionSensorAsync is executed.
        """
        mock_hook.return_value.check_partition_exists.return_value = False
        task = HivePartitionSensorAsync(
            task_id="task-id",
            table=TEST_TABLE,
            partition=TEST_PARTITION,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, HivePartitionTrigger), "Trigger is not a HivePartitionTrigger"

    def test_hive_partition_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""
        task = HivePartitionSensorAsync(
            task_id="task-id",
            table=TEST_TABLE,
            partition=TEST_PARTITION,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

    def test_hive_partition_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""
        task = HivePartitionSensorAsync(
            task_id="task-id",
            table=TEST_TABLE,
            partition=TEST_PARTITION,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
        mock_log_info.assert_called_with(
            "Success criteria met. Found partition %s in table: %s", TEST_PARTITION, TEST_TABLE
        )

    def test_hive_partition_sensor_async_execute_failure_no_event(self, context):
        """Tests that an AirflowException is raised in case of no event"""
        task = HivePartitionSensorAsync(
            task_id="task-id",
            table=TEST_TABLE,
            partition=TEST_PARTITION,
            metastore_conn_id=TEST_METASTORE_CONN_ID,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(context=None, event=None)
