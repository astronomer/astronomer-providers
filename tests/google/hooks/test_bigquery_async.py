from unittest import mock

import pytest
from gcloud.aio.bigquery import Job

from astronomer_operators.google.hooks.bigquery_async import BigQueryHookAsync

PROJECT_ID = "bq-project"
CREDENTIALS = "bq-credentials"
DATASET_ID = "bq_dataset"
TABLE_ID = "bq_table"
PARTITION_ID = "20200101"
VIEW_ID = "bq_view"
JOB_ID = "1234"
LOCATION = "europe-north1"
TABLE_REFERENCE_REPR = {
    "tableId": TABLE_ID,
    "datasetId": DATASET_ID,
    "projectId": PROJECT_ID,
}


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.hooks.bigquery_async.Session")
async def test_get_job_instance(mock_session):
    hook = BigQueryHookAsync()
    result = await hook.get_job_instance(project_id=PROJECT_ID, job_id=JOB_ID, session=mock_session)
    assert isinstance(result, Job)


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.hooks.bigquery_async.BigQueryHookAsync.get_job_instance")
@mock.patch("astronomer_operators.google.hooks.bigquery_async.Session")
async def test_get_job_status_success(mock_session, mock_job_instance):
    hook = BigQueryHookAsync()
    resp = await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    assert resp == "success"
    mock_job_instance.assert_called_once_with(
        PROJECT_ID, JOB_ID, mock_session.return_value.__aenter__.return_value
    )


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.hooks.bigquery_async.BigQueryHookAsync.get_job_instance")
async def test_get_job_status_oserror(mock_job_instance):
    """Assets that the BigQueryHookAsync returns a pending response when OSError is raised"""
    mock_job_instance.return_value.result.side_effect = OSError()
    hook = BigQueryHookAsync()
    job_status = await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    assert job_status == "pending"


@pytest.mark.asyncio
@mock.patch("astronomer_operators.google.hooks.bigquery_async.BigQueryHookAsync.get_job_instance")
async def test_get_job_status_exception(mock_job_instance, caplog):
    """Assets that the logging is done correctly when BigQueryHookAsync raises Exception"""
    mock_job_instance.return_value.result.side_effect = Exception()
    hook = BigQueryHookAsync()
    await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    assert "Query execution finished with errors..." in caplog.text
