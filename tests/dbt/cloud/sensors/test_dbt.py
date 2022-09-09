from unittest import mock

import pytest
from airflow import AirflowException
from airflow.exceptions import TaskDeferred

from astronomer.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensorAsync
from astronomer.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger


class TestDbtCloudJobRunSensorAsync:
    TASK_ID = "dbt_cloud_run_job"
    CONN_ID = "dbt_cloud_default"
    DBT_RUN_ID = 1234
    TIMEOUT = 300

    def test_dbt_job_run_sensor_async(self, context):
        """Assert execute method defer for Dbt cloud job run status sensors"""
        task = DbtCloudJobRunSensorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, DbtCloudRunJobTrigger), "Trigger is not a DbtCloudRunJobTrigger"

    def test_dbt_job_run_sensor_async_execute_complete_success(self):
        """Assert execute_complete log success message when trigger fire with target status"""
        task = DbtCloudJobRunSensorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
        )

        msg = f"Job run {self.DBT_RUN_ID} has completed successfully."
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(
                context={}, event={"status": "success", "message": msg, "run_id": self.DBT_RUN_ID}
            )
        mock_log_info.assert_called_with(msg)

    @pytest.mark.parametrize(
        "mock_status, mock_message",
        [
            ("cancelled", "Job run 1234 has been cancelled."),
            ("error", "Job run 1234 has failed."),
        ],
    )
    def test_dbt_job_run_sensor_async_execute_complete_failure(self, mock_status, mock_message):
        """Assert execute_complete method to raise exception on the cancelled and error status"""
        task = DbtCloudJobRunSensorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            run_id=self.DBT_RUN_ID,
            timeout=self.TIMEOUT,
        )
        with pytest.raises(AirflowException):
            task.execute_complete(
                context={}, event={"status": mock_status, "message": mock_message, "run_id": self.DBT_RUN_ID}
            )
