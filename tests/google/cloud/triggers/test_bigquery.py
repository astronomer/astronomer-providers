import asyncio
import logging
from unittest import mock

import pytest
from aiohttp import ClientResponseError, RequestInfo
from airflow.triggers.base import TriggerEvent
from gcloud.aio.bigquery import Table
from multidict import CIMultiDict
from yarl import URL

from astronomer.providers.google.cloud.hooks.bigquery import BigQueryTableHookAsync
from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryGetDataTrigger,
    BigQueryInsertJobTrigger,
    BigQueryIntervalCheckTrigger,
    BigQueryTableExistenceTrigger,
    BigQueryValueCheckTrigger,
)

TEST_CONN_ID = "bq_default"
TEST_JOB_ID = "1234"
RUN_ID = "1"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET_ID = "bq_dataset"
TEST_TABLE_ID = "bq_table"
POLLING_PERIOD_SECONDS = 4.0
TEST_SQL_QUERY = "SELECT count(*) from Any"
TEST_PASS_VALUE = 2
TEST_TOLERANCE = 1
TEST_FIRST_JOB_ID = "5678"
TEST_SECOND_JOB_ID = "6789"
TEST_METRIC_THRESHOLDS = {}
TEST_DATE_FILTER_COLUMN = "ds"
TEST_DAYS_BACK = -7
TEST_RATIO_FORMULA = "max_over_min"
TEST_IGNORE_ZERO = True
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_HOOK_PARAMS = {}

INSERT_JOB_PARAM = pytest.param(
    BigQueryInsertJobTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=0.0,
    ),
    "astronomer.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger",
    id="BigQueryInsertJobTrigger",
)
GET_DATA_PARAM = pytest.param(
    BigQueryGetDataTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=0.0,
    ),
    "astronomer.providers.google.cloud.triggers.bigquery.BigQueryGetDataTrigger",
    id="BigQueryGetDataTrigger",
)
INTERVAL_CHECK_PARAM = pytest.param(
    BigQueryIntervalCheckTrigger(
        conn_id=TEST_CONN_ID,
        first_job_id=TEST_FIRST_JOB_ID,
        second_job_id=TEST_SECOND_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        table=TEST_TABLE_ID,
        metrics_thresholds=TEST_METRIC_THRESHOLDS,
        date_filter_column=TEST_DATE_FILTER_COLUMN,
        days_back=TEST_DAYS_BACK,
        ratio_formula=TEST_RATIO_FORMULA,
        ignore_zero=TEST_IGNORE_ZERO,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=0.0,
    ),
    "astronomer.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger",
    id="BigQueryIntervalCheckTrigger",
)
VALUE_CHECK_PARAM = pytest.param(
    BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        sql=TEST_SQL_QUERY,
        pass_value=TEST_PASS_VALUE,
        tolerance=TEST_TOLERANCE,
        poll_interval=0.0,
    ),
    "astronomer.providers.google.cloud.triggers.bigquery.BigQueryValueCheckTrigger",
    id="BigQueryValueCheckTrigger",
)
TABLE_EXISTENCE_PARAM = pytest.param(
    BigQueryTableExistenceTrigger(
        conn_id=TEST_GCP_CONN_ID,
        project_id=TEST_DATASET_ID,
        dataset_id=TEST_GCP_PROJECT_ID,
        table_id=TEST_TABLE_ID,
        hook_params=TEST_HOOK_PARAMS,
        poll_interval=0.0,
    ),
    "astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger",
    id="BigQueryTableExistenceTrigger",
)


@pytest.mark.parametrize(
    "trigger, classpath, kwargs",
    [
        pytest.param(
            BigQueryInsertJobTrigger(
                conn_id=TEST_CONN_ID,
                job_id=TEST_JOB_ID,
                project_id=TEST_GCP_PROJECT_ID,
                dataset_id=TEST_DATASET_ID,
                table_id=TEST_TABLE_ID,
                poll_interval=POLLING_PERIOD_SECONDS,
            ),
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "job_id": TEST_JOB_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "dataset_id": TEST_DATASET_ID,
                "table_id": TEST_TABLE_ID,
                "poll_interval": POLLING_PERIOD_SECONDS,
            },
            id="BigQueryInsertJobTrigger",
        ),
        pytest.param(
            BigQueryGetDataTrigger(
                conn_id=TEST_CONN_ID,
                job_id=TEST_JOB_ID,
                project_id=TEST_GCP_PROJECT_ID,
                dataset_id=TEST_DATASET_ID,
                table_id=TEST_TABLE_ID,
                poll_interval=POLLING_PERIOD_SECONDS,
            ),
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryGetDataTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "job_id": TEST_JOB_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "dataset_id": TEST_DATASET_ID,
                "table_id": TEST_TABLE_ID,
                "poll_interval": POLLING_PERIOD_SECONDS,
            },
            id="BigQueryGetDataTrigger",
        ),
        pytest.param(
            BigQueryIntervalCheckTrigger(
                conn_id=TEST_CONN_ID,
                first_job_id=TEST_FIRST_JOB_ID,
                second_job_id=TEST_SECOND_JOB_ID,
                project_id=TEST_GCP_PROJECT_ID,
                table=TEST_TABLE_ID,
                metrics_thresholds=TEST_METRIC_THRESHOLDS,
                date_filter_column=TEST_DATE_FILTER_COLUMN,
                days_back=TEST_DAYS_BACK,
                ratio_formula=TEST_RATIO_FORMULA,
                ignore_zero=TEST_IGNORE_ZERO,
                dataset_id=TEST_DATASET_ID,
                table_id=TEST_TABLE_ID,
                poll_interval=POLLING_PERIOD_SECONDS,
            ),
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "first_job_id": TEST_FIRST_JOB_ID,
                "second_job_id": TEST_SECOND_JOB_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "table": TEST_TABLE_ID,
                "metrics_thresholds": TEST_METRIC_THRESHOLDS,
                "date_filter_column": TEST_DATE_FILTER_COLUMN,
                "days_back": TEST_DAYS_BACK,
                "ratio_formula": TEST_RATIO_FORMULA,
                "ignore_zero": TEST_IGNORE_ZERO,
                "poll_interval": POLLING_PERIOD_SECONDS,
            },
            id="BigQueryIntervalCheckTrigger",
        ),
        pytest.param(
            BigQueryValueCheckTrigger(
                conn_id=TEST_CONN_ID,
                pass_value=TEST_PASS_VALUE,
                job_id=TEST_JOB_ID,
                dataset_id=TEST_DATASET_ID,
                project_id=TEST_GCP_PROJECT_ID,
                sql=TEST_SQL_QUERY,
                table_id=TEST_TABLE_ID,
                tolerance=TEST_TOLERANCE,
                poll_interval=POLLING_PERIOD_SECONDS,
            ),
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryValueCheckTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "pass_value": TEST_PASS_VALUE,
                "job_id": TEST_JOB_ID,
                "dataset_id": TEST_DATASET_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "sql": TEST_SQL_QUERY,
                "table_id": TEST_TABLE_ID,
                "tolerance": TEST_TOLERANCE,
                "poll_interval": POLLING_PERIOD_SECONDS,
            },
            id="BigQueryValueCheckTrigger",
        ),
        pytest.param(
            BigQueryTableExistenceTrigger(
                conn_id=TEST_GCP_CONN_ID,
                project_id=TEST_GCP_PROJECT_ID,
                dataset_id=TEST_DATASET_ID,
                table_id=TEST_TABLE_ID,
                hook_params=TEST_HOOK_PARAMS,
                poll_interval=POLLING_PERIOD_SECONDS,
            ),
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger",
            {
                "conn_id": TEST_GCP_CONN_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "dataset_id": TEST_DATASET_ID,
                "table_id": TEST_TABLE_ID,
                "hook_params": TEST_HOOK_PARAMS,
                "poll_interval": POLLING_PERIOD_SECONDS,
            },
            id="BigQueryTableExistenceTrigger",
        ),
    ],
)
def test_serialization(trigger, classpath, kwargs):
    """
    Asserts that a trigger can correctly serialize its arguments and classpath.
    """
    result_classpath, result_kwargs = trigger.serialize()
    assert result_classpath == classpath
    assert result_kwargs == kwargs


@pytest.mark.parametrize(
    "trigger, logger",
    [INSERT_JOB_PARAM, GET_DATA_PARAM, INTERVAL_CHECK_PARAM, VALUE_CHECK_PARAM],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_trigger_exception(mock_job_status, caplog, trigger, logger):
    """
    Tests BigQueryTrigger fires if the hook raises an exception.
    """
    caplog.set_level(logging.DEBUG, logger=logger)
    mock_job_status.side_effect = Exception("I failed!")

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {
        "status": "error",
        "trigger": trigger.serialize()[1],
        "message": "I failed!",
    }
    assert TriggerEvent(expected) == actual
    assert (
        logger,
        logging.ERROR,
        "Exception occurred while checking for query completion",
    ) in caplog.record_tuples


@pytest.mark.parametrize(
    "trigger, logger",
    [INSERT_JOB_PARAM, GET_DATA_PARAM, INTERVAL_CHECK_PARAM, VALUE_CHECK_PARAM],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_trigger_pending(mock_job_status, caplog, trigger, logger):
    """
    Tests BigQueryTrigger fires when the hook result is still pending.
    """
    caplog.set_level(logging.DEBUG, logger=logger)
    mock_job_status.return_value = "pending"

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {"status": "pending", "trigger": trigger.serialize()[1], "result": None}
    assert TriggerEvent(expected) == actual
    log_message = "Still pending... sleeping for 0.0 seconds."
    assert (logger, logging.DEBUG, log_message) in caplog.record_tuples


@pytest.mark.parametrize("trigger, logger", [INSERT_JOB_PARAM, GET_DATA_PARAM, VALUE_CHECK_PARAM])
@pytest.mark.parametrize(
    "job_status, log_message, message",
    [
        ("error", "Poll response: error", "Error response from hook"),
        ("???", "Unknown poll response: ???", "Unknown response from hook: ???"),
    ],
    ids=["error", "???"],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_job_trigger_error(
    mock_job_status,
    caplog,
    trigger,
    logger,
    job_status,
    log_message,
    message,
):
    """
    Tests BigQueryTrigger fires if the hook returns an error or unknown status.
    """
    caplog.set_level(logging.ERROR, logger=logger)
    mock_job_status.return_value = job_status

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {"status": "error", "trigger": trigger.serialize()[1], "result": None, "message": message}
    assert TriggerEvent(expected) == actual
    assert (logger, logging.ERROR, log_message) in caplog.record_tuples


@pytest.mark.parametrize("trigger, logger", [INTERVAL_CHECK_PARAM])
@pytest.mark.parametrize(
    "first_job_status, second_job_status",
    [("error", "???"), ("???", "error"), ("???", "???")],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_interval_check_trigger_error(
    mock_job_status,
    caplog,
    trigger,
    logger,
    first_job_status,
    second_job_status,
):
    """
    Tests BigQueryIntervalCheckTrigger fires if the hook returns an error or unknown status.
    """
    caplog.set_level(logging.ERROR, logger=logger)
    mock_job_status.side_effect = [first_job_status, second_job_status]

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {
        "status": "error",
        "message": "Error response from hook",
        "trigger": trigger.serialize()[1],
        "result": [first_job_status, second_job_status],
    }
    assert TriggerEvent(expected) == actual
    assert (logger, logging.ERROR, "Poll response: error") in caplog.record_tuples


@pytest.mark.parametrize(
    "job_output, records",
    [
        pytest.param(
            {"totalRows": "0", "totalBytesProcessed": "0"},
            [],
            id="empty",
        ),
        pytest.param(
            {
                "totalRows": "1",
                "totalBytesProcessed": "???",
                "rows": [{"f": [{"v": 123}, {"v": "456"}, {"v": None}]}],
            },
            [[123, "456", None]],
            id="one",
        ),
    ],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_output")
async def test_bigquery_get_data_trigger_success(mock_job_output, mock_job_status, job_output, records):
    """
    Tests that BigQueryGetDataTrigger sends success event with appropriate records.
    """
    mock_job_status.return_value = "success"
    mock_job_output.return_value = {
        "kind": "bigquery#getQueryResultsResponse",
        "etag": "test_etag",
        "schema": {
            "fields": [
                {"name": "value", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
            ]
        },
        "jobReference": {
            "projectId": "test_astronomer-airflow-providers",
            "jobId": "test_jobid",
            "location": "US",
        },
        "jobComplete": True,
        "cacheHit": False,
        **job_output,
    }

    trigger = BigQueryGetDataTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=0.0,
    )
    generator = trigger.run()
    actual = await generator.asend(None)

    expected = {"status": "success", "trigger": trigger.serialize()[1], "result": records}
    assert TriggerEvent(expected) == actual


@pytest.mark.parametrize(
    "get_output_value",
    ["0", "1"],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_output")
async def test_bigquery_interval_check_trigger_success(
    mock_get_job_output, mock_job_status, get_output_value
):
    """
    Tests the BigQueryIntervalCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"
    mock_get_job_output.return_value = get_output_value

    trigger = BigQueryIntervalCheckTrigger(
        conn_id=TEST_CONN_ID,
        first_job_id=TEST_FIRST_JOB_ID,
        second_job_id=TEST_SECOND_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        table=TEST_TABLE_ID,
        metrics_thresholds=TEST_METRIC_THRESHOLDS,
        date_filter_column=TEST_DATE_FILTER_COLUMN,
        days_back=TEST_DAYS_BACK,
        ratio_formula=TEST_RATIO_FORMULA,
        ignore_zero=TEST_IGNORE_ZERO,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {
        "status": "error",
        "message": "The second SQL query returned None",
        "trigger": trigger.serialize()[1],
    }
    assert actual == TriggerEvent(expected)


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_records")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_output")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_value_check_op_trigger_success(mock_job_status, get_job_output, get_records):
    """
    Tests that the BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"
    get_job_output.return_value = {}
    get_records.return_value = [[2], [4]]

    trigger = BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        pass_value=TEST_PASS_VALUE,
        job_id=TEST_JOB_ID,
        dataset_id=TEST_DATASET_ID,
        project_id=TEST_GCP_PROJECT_ID,
        sql=TEST_SQL_QUERY,
        table_id=TEST_TABLE_ID,
        tolerance=TEST_TOLERANCE,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    generator = trigger.run()
    actual = await generator.asend(None)
    assert actual == TriggerEvent({"status": "success", "trigger": trigger.serialize()[1], "result": [2]})


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_value_check_op_trigger_fail(mock_job_status):
    """
    Tests that the BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "dummy"

    trigger = BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        pass_value=TEST_PASS_VALUE,
        job_id=TEST_JOB_ID,
        dataset_id=TEST_DATASET_ID,
        project_id=TEST_GCP_PROJECT_ID,
        sql=TEST_SQL_QUERY,
        table_id=TEST_TABLE_ID,
        tolerance=TEST_TOLERANCE,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {
        "status": "error",
        "message": "Unknown response from hook: dummy",
        "trigger": trigger.serialize()[1],
        "result": None,
    }
    assert TriggerEvent(expected) == actual


@pytest.mark.parametrize("trigger, logger", [TABLE_EXISTENCE_PARAM])
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists")
async def test_table_existence_trigger_exception(mock_table_exists, caplog, trigger, logger):
    """
    Tests BigQueryTableExistenceTrigger fires if the hook raises an exception.
    """
    caplog.set_level(logging.DEBUG, logger=logger)
    mock_table_exists.side_effect = mock.AsyncMock(side_effect=Exception("I failed!"))

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {
        "status": "error",
        "trigger": trigger.serialize()[1],
        "message": "I failed!",
    }
    assert TriggerEvent(expected) == actual
    assert (
        logger,
        logging.ERROR,
        "Exception occurred while checking for query completion",
    ) in caplog.record_tuples


@pytest.mark.parametrize("trigger, logger", [TABLE_EXISTENCE_PARAM])
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists")
async def test_table_existence_trigger_pending(mock_table_exists, caplog, trigger, logger):
    """
    Tests BigQueryTableExistenceTrigger fires when the hook result is still pending.
    """
    caplog.set_level(logging.DEBUG, logger=logger)
    mock_table_exists.side_effect = mock.AsyncMock(return_value=False)

    generator = trigger.run()
    actual = await generator.asend(None)
    expected = {"status": "pending", "trigger": trigger.serialize()[1], "result": None}
    assert TriggerEvent(expected) == actual
    log_message = "Still pending... sleeping for 0.0 seconds."
    assert (logger, logging.DEBUG, log_message) in caplog.record_tuples


@pytest.mark.parametrize("trigger, logger", [TABLE_EXISTENCE_PARAM])
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists")
async def test_big_query_table_existence_trigger_success(mock_table_exists, trigger, logger):
    """
    Tests success case BigQueryTableExistenceTrigger
    """
    mock_table_exists.return_value = True

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "trigger": trigger.serialize()[1], "result": None}) == actual


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger, logger", [TABLE_EXISTENCE_PARAM])
@pytest.mark.parametrize(
    "mock_get_table_client_value, expected_value",
    [
        (
            Table,
            True,
        )
    ],
)
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryTableHookAsync.get_table_client")
async def test_table_exists(
    mock_get_table_client,
    mock_get_table_client_value,
    trigger,
    logger,
    expected_value,
):
    """Test BigQueryTableExistenceTrigger._table_exists async function with mocked value and mocked return value"""
    trigger._get_async_hook = lambda: mock.AsyncMock(BigQueryTableHookAsync)
    mock_get_table_client.return_value = mock_get_table_client_value
    res = await trigger._table_exists()
    assert res == expected_value


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger, logger", [TABLE_EXISTENCE_PARAM])
async def test_table_exists_404(monkeypatch, trigger, logger):
    """Test BigQueryTableExistenceTrigger._table_exists async function with exception and return False"""
    client_response = ClientResponseError(
        history=(),
        request_info=RequestInfo(
            headers=CIMultiDict(),
            real_url=URL("https://example.com"),
            method="GET",
            url=URL("https://example.com"),
        ),
        status=404,
        message="Not Found",
    )
    monkeypatch.setattr(
        trigger,
        "_get_async_hook",
        lambda: mock.Mock(get_table_client=mock.AsyncMock(side_effect=client_response)),
    )
    res = await trigger._table_exists()
    expected_response = False
    assert res == expected_response


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger, logger", [TABLE_EXISTENCE_PARAM])
async def test_table_exists_raise_exception(monkeypatch, trigger, logger):
    """Test BigQueryTableExistenceTrigger._table_exists async function with raise exception"""
    client_response = ClientResponseError(
        history=(),
        request_info=RequestInfo(
            headers=CIMultiDict(),
            real_url=URL("https://example.com"),
            method="GET",
            url=URL("https://example.com"),
        ),
        status=400,
        message="Bad Request",
    )
    monkeypatch.setattr(
        trigger,
        "_get_async_hook",
        lambda: mock.Mock(get_table_client=mock.AsyncMock(side_effect=client_response)),
    )
    with pytest.raises(ClientResponseError):
        await trigger._table_exists()
