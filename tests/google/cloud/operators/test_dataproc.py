from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperatorAsync,
)
from astronomer.providers.google.cloud.triggers.dataproc import DataProcSubmitTrigger

TEST_PROJECT_ID = "test_project_id"
TEST_CLUSTER_NAME = "test_cluster"
TEST_REGION = "us-central1"
TEST_ZONE = "us-central1-a"
TEST_JOB_ID = "test-job"

SPARK_JOB = {
    "reference": {"project_id": TEST_PROJECT_ID},
    "placement": {"cluster_name": TEST_CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["some/random/path"],
        "main_class": "test.main.class",
    },
}


@pytest.fixture()
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("airflow.providers.google.cloud.operators.dataproc.DataprocHook.submit_job")
def test_dataproc_operator_execute_async(mock_submit_job):
    """
    Asserts that a task is deferred and a DataProcSubmitTrigger will be fired
    when the DataprocSubmitJobOperatorAsync is executed.
    """
    mock_submit_job.return_value.reference.job_id = TEST_JOB_ID
    task = DataprocSubmitJobOperatorAsync(
        task_id="task-id", job=SPARK_JOB, region=TEST_REGION, project_id=TEST_PROJECT_ID
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, DataProcSubmitTrigger), "Trigger is not a DataProcSubmitTrigger"


@pytest.mark.parametrize(
    "event",
    [
        ({"status": "error", "message": "test failure message"}),
        (None),
    ],
)
@mock.patch("airflow.providers.google.cloud.operators.dataproc.DataprocHook.submit_job")
def test_dataproc_operator_execute_failure_async(mock_submit_job, event):
    """Tests that an AirflowException is raised in case of error event"""
    mock_submit_job.return_value.reference.job_id = TEST_JOB_ID
    task = DataprocSubmitJobOperatorAsync(
        task_id="task-id", job=SPARK_JOB, region=TEST_REGION, project_id=TEST_PROJECT_ID
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event=event)


@mock.patch("airflow.providers.google.cloud.operators.dataproc.DataprocHook.submit_job")
def test_dataproc_operator_execute_success_async(mock_submit_job):
    """Tests response message in case of success event"""
    mock_submit_job.return_value.reference.job_id = TEST_JOB_ID
    task = DataprocSubmitJobOperatorAsync(
        task_id="task-id", job=SPARK_JOB, region=TEST_REGION, project_id=TEST_PROJECT_ID
    )
    assert task.execute_complete(context=None, event={"status": "success", "message": "success"})
