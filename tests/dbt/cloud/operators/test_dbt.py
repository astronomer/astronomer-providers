from datetime import datetime
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.dbt.cloud.hooks.dbt import (
    DbtCloudJobRunException,
    DbtCloudJobRunStatus,
)
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperatorAsync
from astronomer.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger


class TestDbtCloudRunJobOperatorAsync:
    TASK_ID = "dbt_cloud_run_job"
    CONN_ID = "dbt_cloud_default"
    DBT_RUN_ID = 1234
    CHECK_INTERVAL = 10
    TIMEOUT = 300
    DEFAULT_DATE = timezone.datetime(2021, 1, 1)
    dag = DAG("test_dbt_cloud_job_run_op", start_date=DEFAULT_DATE)

    def create_context(self, task):
        execution_date = datetime(2022, 1, 1, 0, 0, 0)
        dag_run = DagRun(
            dag_id=self.dag.dag_id,
            execution_date=execution_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
        )
        task_instance = TaskInstance(task=task)
        task_instance.dag_run = dag_run
        task_instance.dag_id = self.dag.dag_id
        task_instance.xcom_push = mock.Mock()
        return {
            "dag": self.dag,
            "run_id": dag_run.run_id,
            "task": task,
            "ti": task_instance,
            "task_instance": task_instance,
        }

    @mock.patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
        return_value=DbtCloudJobRunStatus.SUCCESS.value,
    )
    @mock.patch("astronomer.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperatorAsync.defer")
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run")
    def test_dbt_run_job_op_async_succeeded_before_deferred(
        self, mock_trigger_job_run, mock_dbt_hook, mock_defer, mock_job_run_status
    ):
        dbt_op = DbtCloudRunJobOperatorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=f"{self.TASK_ID}",
            job_id=self.DBT_RUN_ID,
            check_interval=self.CHECK_INTERVAL,
            timeout=self.TIMEOUT,
            dag=self.dag,
        )
        dbt_op.execute(self.create_context(dbt_op))
        assert not mock_defer.called

    @pytest.mark.parametrize(
        "status", (DbtCloudJobRunStatus.CANCELLED.value, DbtCloudJobRunStatus.ERROR.value)
    )
    @mock.patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
    )
    @mock.patch("astronomer.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperatorAsync.defer")
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run")
    def test_dbt_run_job_op_async_failed_before_deferred(
        self, mock_trigger_job_run, mock_dbt_hook, mock_defer, mock_job_run_status, status
    ):
        mock_job_run_status.return_value = status
        dbt_op = DbtCloudRunJobOperatorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=f"{self.TASK_ID}{status}",
            job_id=self.DBT_RUN_ID,
            check_interval=self.CHECK_INTERVAL,
            timeout=self.TIMEOUT,
            dag=self.dag,
        )
        with pytest.raises(DbtCloudJobRunException):
            dbt_op.execute(self.create_context(dbt_op))
        assert not mock_defer.called

    @pytest.mark.parametrize(
        "status",
        (
            DbtCloudJobRunStatus.QUEUED.value,
            DbtCloudJobRunStatus.STARTING.value,
            DbtCloudJobRunStatus.RUNNING.value,
        ),
    )
    @mock.patch(
        "airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_job_run_status",
    )
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.get_connection")
    @mock.patch("airflow.providers.dbt.cloud.hooks.dbt.DbtCloudHook.trigger_job_run")
    def test_dbt_run_job_op_async(self, mock_trigger_job_run, mock_dbt_hook, mock_job_run_status, status):
        """
        Asserts that a task is deferred and an DbtCloudRunJobTrigger will be fired
        when the DbtCloudRunJobOperatorAsync is provided with all required arguments
        """
        mock_job_run_status.return_value = status
        dbt_op = DbtCloudRunJobOperatorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=f"{self.TASK_ID}{status}",
            job_id=self.DBT_RUN_ID,
            check_interval=self.CHECK_INTERVAL,
            timeout=self.TIMEOUT,
            dag=self.dag,
        )
        with pytest.raises(TaskDeferred) as exc:
            dbt_op.execute(self.create_context(dbt_op))

        assert isinstance(exc.value.trigger, DbtCloudRunJobTrigger), "Trigger is not a DbtCloudRunJobTrigger"

    def test_dbt_run_job_op_with_exception(self):
        """Test DbtCloudRunJobOperatorAsync to raise exception"""
        dbt_op = DbtCloudRunJobOperatorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            job_id=self.DBT_RUN_ID,
            check_interval=self.CHECK_INTERVAL,
            timeout=self.TIMEOUT,
        )
        with pytest.raises(AirflowException):
            dbt_op.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    @pytest.mark.parametrize(
        "mock_event",
        [
            ({"status": "success", "message": "Job run 48617 has completed successfully.", "run_id": 1234}),
        ],
    )
    def test_dbt_job_execute_complete(self, mock_event):
        """Test DbtCloudRunJobOperatorAsync by mocking the success response and assert the log and return value"""
        dbt_op = DbtCloudRunJobOperatorAsync(
            dbt_cloud_conn_id=self.CONN_ID,
            task_id=self.TASK_ID,
            job_id=self.DBT_RUN_ID,
            check_interval=self.CHECK_INTERVAL,
            timeout=self.TIMEOUT,
        )

        with mock.patch.object(dbt_op.log, "info") as mock_log_info:
            assert dbt_op.execute_complete(context=None, event=mock_event) == self.DBT_RUN_ID

        mock_log_info.assert_called_with("Job run 48617 has completed successfully.")
