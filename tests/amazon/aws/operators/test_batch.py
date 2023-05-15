from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook

from astronomer.providers.amazon.aws.operators.batch import BatchOperatorAsync
from astronomer.providers.amazon.aws.triggers.batch import BatchOperatorTrigger
from tests.utils.airflow_util import create_context


class TestBatchOperatorAsync:
    JOB_NAME = "51455483-c62c-48ac-9b88-53a6a725baa3"
    JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
    MAX_RETRIES = 2
    STATUS_RETRIES = 3
    RESPONSE_WITHOUT_FAILURES = {
        "jobName": JOB_NAME,
        "jobId": JOB_ID,
    }

    @mock.patch("astronomer.providers.amazon.aws.operators.batch.BatchOperatorAsync.defer")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientHook.get_job_description")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def test_batch_op_async_succeeded_before_defer(self, get_client_type_mock, get_job_description, defer):
        get_client_type_mock.return_value.submit_job.return_value = self.RESPONSE_WITHOUT_FAILURES
        get_job_description.return_value = {"status": BatchClientHook.SUCCESS_STATE}
        task = BatchOperatorAsync(
            task_id="task",
            job_name=self.JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            tags={},
        )
        context = create_context(task)
        task.execute(context)
        assert not defer.called

    @pytest.mark.parametrize("status", (BatchClientHook.FAILURE_STATE, "Unexpected status"))
    @mock.patch("astronomer.providers.amazon.aws.operators.batch.BatchOperatorAsync.defer")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientHook.get_job_description")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def test_batch_op_async_failed_before_defer(
        self, get_client_type_mock, get_job_description, defer, status
    ):
        get_client_type_mock.return_value.submit_job.return_value = self.RESPONSE_WITHOUT_FAILURES
        get_job_description.return_value = {"status": status}
        task = BatchOperatorAsync(
            task_id="task",
            job_name=self.JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            tags={},
        )
        context = create_context(task)
        with pytest.raises(AirflowException):
            task.execute(context)
        assert not defer.called

    @pytest.mark.parametrize("status", BatchClientHook.INTERMEDIATE_STATES)
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.BatchClientHook.get_job_description")
    @mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
    def test_batch_op_async(self, get_client_type_mock, get_job_description, status):
        get_client_type_mock.return_value.submit_job.return_value = self.RESPONSE_WITHOUT_FAILURES
        get_job_description.return_value = {"status": status}
        task = BatchOperatorAsync(
            task_id="task",
            job_name=self.JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            tags={},
        )
        context = create_context(task)
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, BatchOperatorTrigger), "Trigger is not a BatchOperatorTrigger"

    def test_batch_op_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        task = BatchOperatorAsync(
            task_id="task",
            job_name=self.JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            tags={},
        )
        with pytest.raises(AirflowException) as exc_info:
            task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

        assert str(exc_info.value) == "test failure message"

    @pytest.mark.parametrize(
        "event",
        [{"status": "success", "message": f"AWS Batch job ({JOB_ID}) succeeded"}],
    )
    def test_batch_op_async_execute_complete(self, caplog, event):
        """Tests that execute_complete method returns None and that it prints expected log"""
        task = BatchOperatorAsync(
            task_id="task",
            job_name=self.JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            tags={},
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            assert task.execute_complete(context=None, event=event) is None

        mock_log_info.assert_called_with(f"AWS Batch job ({self.JOB_ID}) succeeded")

    @mock.patch("astronomer.providers.amazon.aws.operators.batch.BatchOperatorAsync.submit_job")
    def test_batch_op_raises_exception_before_deferral_if_job_id_unset(self, mock_submit_job):
        """
        Test that an AirflowException is raised if job_id is not set before deferral by mocking the submit_job
        method which sets the job_id attribute of the instance.
        """
        task = BatchOperatorAsync(
            task_id="task",
            job_name=self.JOB_NAME,
            job_queue="queue",
            job_definition="hello-world",
            max_retries=self.MAX_RETRIES,
            status_retries=self.STATUS_RETRIES,
            parameters=None,
            overrides={},
            array_properties=None,
            aws_conn_id="aws_default",
            region_name="eu-west-1",
            tags={},
        )
        context = create_context(task)
        with pytest.raises(AirflowException) as exc:
            task.execute(context)
        assert "AWS Batch job - job_id was not found" in str(exc.value)
