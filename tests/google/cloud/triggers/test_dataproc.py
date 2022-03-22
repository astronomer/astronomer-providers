import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent
from google.cloud.dataproc_v1 import Job
from google.cloud.dataproc_v1.types import JobStatus

from astronomer.providers.google.cloud.hooks.dataproc import DataprocHookAsync
from astronomer.providers.google.cloud.triggers.dataproc import DataProcSubmitTrigger

TEST_PROJECT_ID = "test_project_id"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_CLUSTER_NAME = "test_ccluster"
TEST_REGION = "us-central1"
TEST_ZONE = "us-central1-a"
TEST_JOB_ID = "test-job"
TEST_POLLING_INTERVAL = 3.0


def test_dataproc_submit_trigger_serialization():
    """
    Asserts that the DataProcSubmitTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = DataProcSubmitTrigger(
        gcp_conn_id=TEST_GCP_CONN_ID,
        dataproc_job_id=TEST_JOB_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        polling_interval=TEST_POLLING_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger"
    assert kwargs == {
        "project_id": TEST_PROJECT_ID,
        "dataproc_job_id": TEST_JOB_ID,
        "region": TEST_REGION,
        "polling_interval": TEST_POLLING_INTERVAL,
        "gcp_conn_id": TEST_GCP_CONN_ID,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [
        ({"status": "success", "message": "Job completed successfully"}),
        ({"status": "error", "message": "Job Failed"}),
    ],
)
@mock.patch("astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger._get_job_status")
async def test_dataproc_submit_return_success_and_failure(mock_get_job_status, status):
    """Tests that the DataProcSubmitTrigger is success case and also error case"""
    mock_get_job_status.return_value = status

    trigger = DataProcSubmitTrigger(
        dataproc_job_id=TEST_JOB_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        polling_interval=TEST_POLLING_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent(status) in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger._get_job_status")
async def test_dataproc_submit_return_pending(mock_get_job_status):
    """Tests that the DataProcSubmitTrigger is in pending state"""
    mock_get_job_status.return_value = {"status": "pending"}

    trigger = DataProcSubmitTrigger(
        dataproc_job_id=TEST_JOB_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        polling_interval=TEST_POLLING_INTERVAL,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.dataproc.DataProcSubmitTrigger._get_job_status")
async def test_dataproc_submit_return_exception(mock_get_job_status):
    """Tests that the DataProcSubmitTrigger throws an exception"""
    mock_get_job_status.side_effect = Exception("Test exception")

    trigger = DataProcSubmitTrigger(
        dataproc_job_id=TEST_JOB_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        polling_interval=TEST_POLLING_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state, response",
    [
        (JobStatus.State.DONE, {"status": "success", "message": "Job completed successfully"}),
        (JobStatus.State.ERROR, {"status": "error", "message": "Job Failed"}),
        (JobStatus.State.CANCELLED, {"status": "error", "message": "Job got cancelled"}),
        (JobStatus.State.ATTEMPT_FAILURE, {"status": "pending", "message": "Job is in pending state"}),
        (JobStatus.State.SETUP_DONE, {"status": "pending", "message": "Job is in pending state"}),
    ],
)
async def test_dataproc_get_job_status(state, response):
    """Tests that the get job status gives appropriate status for the job"""
    hook = mock.AsyncMock(DataprocHookAsync)
    get_job_instance = mock.AsyncMock(Job)
    hook.get_job = get_job_instance
    hook.get_job.return_value.status.state = state
    trigger = DataProcSubmitTrigger(
        dataproc_job_id=TEST_JOB_ID,
        project_id=TEST_PROJECT_ID,
        region=TEST_REGION,
        polling_interval=TEST_POLLING_INTERVAL,
    )
    res = await trigger._get_job_status(hook)
    assert res == response
