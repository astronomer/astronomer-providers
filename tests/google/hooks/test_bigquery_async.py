from unittest import mock
from unittest.mock import AsyncMock

import pytest

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
@mock.patch("astronomer_operators.google.hooks.bigquery_async.BigQueryHookAsync.get_job_instance")
@mock.patch("aiohttp.client.ClientSession")
async def test_get_job_status(mock_job_instance, mock_session):
    print("Entered test_get_job_status")
    hook = BigQueryHookAsync()
    m = AsyncMock()
    mock_session.return_value = m
    resp = await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
    print(resp)
    mock_job_instance.assert_called_once_with(PROJECT_ID, JOB_ID, m)
