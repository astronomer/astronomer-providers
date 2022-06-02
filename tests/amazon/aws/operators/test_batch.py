from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.operators.batch import BatchOperatorAsync
from astronomer.providers.amazon.aws.triggers.batch import BatchOperatorTrigger

JOB_NAME = "51455483-c62c-48ac-9b88-53a6a725baa3"
JOB_ID = "8ba9d676-4108-4474-9dca-8bbac1da9b19"
MAX_RETRIES = 2
STATUS_RETRIES = 3

RESPONSE_WITHOUT_FAILURES = {
    "jobName": JOB_NAME,
    "jobId": JOB_ID,
}


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("airflow.providers.amazon.aws.hooks.batch_client.AwsBaseHook.get_client_type")
def test_batch_op_async(get_client_type_mock):
    get_client_type_mock.return_value.submit_job.return_value = RESPONSE_WITHOUT_FAILURES
    task = BatchOperatorAsync(
        task_id="task",
        job_name=JOB_NAME,
        job_queue="queue",
        job_definition="hello-world",
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters=None,
        overrides={},
        array_properties=None,
        aws_conn_id="airflow_test",
        region_name="eu-west-1",
        tags={},
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, BatchOperatorTrigger), "Trigger is not a BatchOperatorTrigger"


def test_batch_op_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""

    task = BatchOperatorAsync(
        task_id="task",
        job_name=JOB_NAME,
        job_queue="queue",
        job_definition="hello-world",
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters=None,
        overrides={},
        array_properties=None,
        aws_conn_id="airflow_test",
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
def test_batch_op_async_execute_complete(caplog, event):
    """Tests that execute_complete method returns None and that it prints expected log"""
    task = BatchOperatorAsync(
        task_id="task",
        job_name=JOB_NAME,
        job_queue="queue",
        job_definition="hello-world",
        max_retries=MAX_RETRIES,
        status_retries=STATUS_RETRIES,
        parameters=None,
        overrides={},
        array_properties=None,
        aws_conn_id="airflow_test",
        region_name="eu-west-1",
        tags={},
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        assert task.execute_complete(context=None, event=event) is None

    mock_log_info.assert_called_with(f"AWS Batch job ({JOB_ID}) succeeded")
