from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from gcloud.aio.bigquery import Job, Table

from astronomer.providers.google.cloud.hooks.bigquery import (
    BigQueryHookAsync,
    BigQueryTableHookAsync,
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
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.ClientSession")
async def test_get_job_instance(mock_session):
    hook = BigQueryHookAsync()
    result = await hook.get_job_instance(project_id=PROJECT_ID, job_id=JOB_ID, session=mock_session)
    assert isinstance(result, Job)


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_instance")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.ClientSession")
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
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.ClientSession")
async def test_get_job_output_assert_once_with(mock_session, mock_job_instance):
    hook = BigQueryHookAsync()
    await hook.get_job_output(job_id=JOB_ID, project_id=PROJECT_ID)
    mock_job_instance.assert_called_once_with(
        PROJECT_ID, JOB_ID, mock_session.return_value.__aenter__.return_value
    )


def test_interval_check_for_airflow_exception():
    """
    Assert that check return AirflowException
    """
    hook = BigQueryHookAsync()

    row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
        None,
        "0",
        {"COUNT(*)": 1.5},
        True,
        "max_over_min",
    )
    with pytest.raises(AirflowException):
        hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)

    row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
        "0",
        None,
        {"COUNT(*)": 1.5},
        True,
        "max_over_min",
    )
    with pytest.raises(AirflowException):
        hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)

    row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
        "1",
        "1",
        {"COUNT(*)": 0},
        True,
        "max_over_min",
    )
    with pytest.raises(AirflowException):
        hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)


def test_interval_check_for_success():
    """
    Assert that check return None
    """
    hook = BigQueryHookAsync()

    row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
        "0",
        "0",
        {"COUNT(*)": 1.5},
        True,
        "max_over_min",
    )
    response = hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)
    assert response is None


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


@pytest.mark.parametrize(
    "records,pass_value,tolerance", [(["str"], "str", None), ([2], 2, None), ([0], 2, 1), ([4], 2, 1)]
)
def test_value_check_success(records, pass_value, tolerance):
    """
    Assert that value_check method execution succeed
    """
    hook = BigQueryHookAsync()
    query = "SELECT COUNT(*) from Any"

    response = hook.value_check(query, pass_value, records, tolerance)

    assert response is None


@pytest.mark.parametrize(
    "records,pass_value,tolerance",
    [([], "", None), (["str"], "str1", None), ([2], 21, None), ([5], 2, 1), (["str"], 2, None)],
)
def test_value_check_fail(records, pass_value, tolerance):
    """Assert that check raise AirflowException"""
    hook = BigQueryHookAsync()
    query = "SELECT COUNT(*) from Any"

    with pytest.raises(AirflowException) as ex:
        hook.value_check(query, pass_value, records, tolerance)
    assert isinstance(ex.value, AirflowException)


@pytest.mark.parametrize(
    "records,pass_value,tolerance, expected",
    [
        ([2.0], 2.0, None, [True]),
        ([2.0], 2.1, None, [False]),
        ([2.0], 2.0, 0.5, [True]),
        ([1.0], 2.0, 0.5, [True]),
        ([3.0], 2.0, 0.5, [True]),
        ([0.9], 2.0, 0.5, [False]),
        ([3.1], 2.0, 0.5, [False]),
    ],
)
def test_get_numeric_matches(records, pass_value, tolerance, expected):
    """Assert the if response list have all element match with pass_value with tolerance"""

    assert BigQueryHookAsync._get_numeric_matches(records, pass_value, tolerance) == expected


@pytest.mark.parametrize("test_input,expected", [(5.0, 5.0), (5, 5.0), ("5", 5), ("str", "str")])
def test_convert_to_float_if_possible(test_input, expected):
    """
    Assert that type casting succeed for the possible value
    Otherwise return the same value
    """

    assert BigQueryHookAsync._convert_to_float_if_possible(test_input) == expected


@pytest.mark.asyncio
@mock.patch("aiohttp.client.ClientSession")
async def test_get_table_client(mock_session):
    """Test get_table_client async function and check whether the return value is a Table instance object"""
    hook = BigQueryTableHookAsync()
    result = await hook.get_table_client(
        dataset=DATASET_ID, project_id=PROJECT_ID, table_id=TABLE_ID, session=mock_session
    )
    assert isinstance(result, Table)
