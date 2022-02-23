from unittest import mock

import pytest
from gcloud.aio.bigquery import Job

from astronomer.providers.google.cloud.hooks.bigquery import (
    BigQueryHookAsync,
    _BigQueryHook,
)

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


class _BigQueryBaseTestClass:
    def setup_method(self) -> None:
        class MockedBigQueryHook(_BigQueryHook):
            def _get_credentials_and_project_id(self):
                return CREDENTIALS, PROJECT_ID

        self.hook = MockedBigQueryHook()


class TestBigQueryHookMethods(_BigQueryBaseTestClass):
    @pytest.mark.parametrize("nowait", [True, False])
    @mock.patch("astronomer.providers.google.cloud.hooks.bigquery.QueryJob")
    @mock.patch("astronomer.providers.google.cloud.hooks.bigquery._BigQueryHook.get_client")
    def test_insert_job(self, mock_client, mock_query_job, nowait):
        job_conf = {
            "query": {
                "query": "SELECT * FROM test",
                "useLegacySql": "False",
            }
        }
        mock_query_job._JOB_TYPE = "query"

        self.hook.insert_job(
            configuration=job_conf, job_id=JOB_ID, project_id=PROJECT_ID, location=LOCATION, nowait=nowait
        )

        mock_client.assert_called_once_with(
            project_id=PROJECT_ID,
            location=LOCATION,
        )

        mock_query_job.from_api_repr.assert_called_once_with(
            {
                "configuration": job_conf,
                "jobReference": {"jobId": JOB_ID, "projectId": PROJECT_ID, "location": LOCATION},
            },
            mock_client.return_value,
        )
        if nowait:
            mock_query_job.from_api_repr.return_value._begin.assert_called_once()
            mock_query_job.from_api_repr.return_value.result.assert_not_called()
        else:
            mock_query_job.from_api_repr.return_value._begin.assert_not_called()
            mock_query_job.from_api_repr.return_value.result.assert_called_once()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.Session")
async def test_get_job_instance(mock_session):
    hook = BigQueryHookAsync()
    result = await hook.get_job_instance(project_id=PROJECT_ID, job_id=JOB_ID, session=mock_session)
    assert isinstance(result, Job)


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_instance")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.Session")
async def test_get_job_status_success(mock_session, mock_job_instance):
    hook = BigQueryHookAsync()
    resp = await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    assert resp == "success"
    mock_job_instance.assert_called_once_with(
        PROJECT_ID, JOB_ID, mock_session.return_value.__aenter__.return_value
    )


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_instance")
async def test_get_job_status_oserror(mock_job_instance):
    """Assets that the BigQueryHookAsync returns a pending response when OSError is raised"""
    mock_job_instance.return_value.result.side_effect = OSError()
    hook = BigQueryHookAsync()
    job_status = await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    assert job_status == "pending"


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_instance")
async def test_get_job_status_exception(mock_job_instance, caplog):
    """Assets that the logging is done correctly when BigQueryHookAsync raises Exception"""
    mock_job_instance.return_value.result.side_effect = Exception()
    hook = BigQueryHookAsync()
    await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    assert "Query execution finished with errors..." in caplog.text


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_instance")
async def test_get_job_output(mock_job_instance):
    """
    Tests to check if a particular object in Google Cloud Storage
    is found or not
    """
    response = {
        "kind": "bigquery#tableDataList",
        "etag": "test_etag",
        "schema": {"fields": [{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"}]},
        "jobReference": {
            "projectId": "test_astronomer-airflow-providers",
            "jobId": "test_jobid",
            "location": "US",
        },
        "totalRows": "10",
        "rows": [{"f": [{"v": "42"}, {"v": "monthy python"}]}, {"f": [{"v": "42"}, {"v": "fishy fish"}]}],
        "totalBytesProcessed": "0",
        "jobComplete": True,
        "cacheHit": False,
    }
    hook = BigQueryHookAsync()
    mock_job_client = mock.AsyncMock(Job)
    mock_job_instance.return_value = mock_job_client
    mock_job_client.get_query_results.return_value = response
    resp = await hook.get_job_output(job_id=JOB_ID, project_id=PROJECT_ID)
    assert resp == response
