from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperatorAsync
from astronomer.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger


class TestDbtCloudRunJobOperatorAsync:
    TASK_ID = "dbt_cloud_run_job"

    def test_dbt_run_job_op_async(self, context):
        """
        Asserts that a task is deferred and an DbtCloudRunJobTrigger will be fired
        when the DbtCloudRunJobOperatorAsync is provided with all required arguments
        """
        dbt_op = DbtCloudRunJobOperatorAsync(
            task_id=self.TASK_ID,
            job_id=1234,
            check_interval=10,
            timeout=300,
        )

        with pytest.raises(TaskDeferred) as exc:
            dbt_op.execute(context)

        assert isinstance(exc.value.trigger, DbtCloudRunJobTrigger), "Trigger is not a DbtCloudRunJobTrigger"

    def test_dbt_run_job_op_with_exception(self, context):
        """Test DbtCloudRunJobOperatorAsync with error"""
        dbt_op = DbtCloudRunJobOperatorAsync(
            task_id=self.TASK_ID,
            job_id=1234,
            check_interval=10,
            timeout=300,
        )
        with pytest.raises(AirflowException):
            dbt_op.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    @pytest.mark.parametrize(
        "mock_event",
        [
            None,
            ({"status": "success", "message": "Job run 48617 has completed successfully."}),
        ],
    )
    def test_dbt_job_execute_complete(self, context, mock_event):
        dbt_op = DbtCloudRunJobOperatorAsync(
            task_id=self.TASK_ID,
            job_id=1234,
            check_interval=10,
            timeout=300,
        )

        with mock.patch.object(dbt_op.log, "info") as mock_log_info:
            assert dbt_op.execute_complete(context=None, event=mock_event) is None

        mock_log_info.assert_called_with("Job run 48617 has completed successfully.")
