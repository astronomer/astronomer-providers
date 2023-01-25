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


class TestBigQueryTableExistenceSensorAsync:
    SENSOR = BigQueryTableExistenceSensorAsync(
        task_id="bq_check_table",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
    )

    def test_big_query_table_existence_sensor_async(self, context):
        """
        Asserts that a task is deferred and a BigQueryTableExistenceTrigger will be fired
        when the BigQueryTableExistenceSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.SENSOR.execute(context)
        assert isinstance(
            exc.value.trigger, BigQueryTableExistenceTrigger
        ), "Trigger is not a BigQueryTableExistenceTrigger"

    def test_big_query_table_existence_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_big_query_table_existence_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""

        table_uri = f"{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}"
        with mock.patch.object(self.SENSOR.log, "info") as mock_log_info:
            self.SENSOR.execute_complete(
                context=None, event={"status": "success", "message": "Job completed"}
            )
        mock_log_info.assert_called_with("Sensor checks existence of table: %s", table_uri)

    def test_redshift_sensor_async_execute_complete_event_none(self):
        """Asserts that logging occurs as expected"""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(context=None, event=None)

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for BigQueryTableExistenceSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            BigQueryTableExistenceSensorAsync(
                task_id="task-id",
                project_id=PROJECT_ID,
                dataset_id=DATASET_NAME,
                table_id=TABLE_NAME,
                polling_interval=5.0,
            )
