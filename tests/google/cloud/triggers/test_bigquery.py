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
    BigQueryIntervalCheckTrigger,
    BigQueryTableExistenceTrigger,
    BigQueryTrigger,
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

BIG_QUERY_TRIGGER_LOGGER = "astronomer.providers.google.cloud.triggers.bigquery.BigQueryTrigger"


@pytest.mark.parametrize(
    "trigger, classpath, kwargs",
    [
        pytest.param(
            BigQueryTrigger(
                conn_id=TEST_CONN_ID,
                job_id=TEST_JOB_ID,
                project_id=TEST_GCP_PROJECT_ID,
                dataset_id=TEST_DATASET_ID,
                table_id=TEST_TABLE_ID,
                poll_interval=POLLING_PERIOD_SECONDS,
            ),
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "job_id": TEST_JOB_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "dataset_id": TEST_DATASET_ID,
                "table_id": TEST_TABLE_ID,
                "poll_interval": POLLING_PERIOD_SECONDS,
            },
            id="BigQueryTrigger",
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
    "status, event_data, log_record",
    [
        pytest.param(
            "pending",
            {
                "status": "pending",
                "job_id": TEST_JOB_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "poll_interval": 0.0,
            },
            (BIG_QUERY_TRIGGER_LOGGER, logging.DEBUG, "Query is still running... sleeping for 0.0 seconds."),
            id="pending",
        ),
        pytest.param(
            "success",
            {
                "status": "success",
                "job_id": TEST_JOB_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "poll_interval": 0.0,
            },
            (BIG_QUERY_TRIGGER_LOGGER, logging.DEBUG, "Response from hook: success"),
            id="success",
        ),
        pytest.param(
            "???",
            {
                "status": "error",
                "message": "Unknown response: '???'",
                "job_id": TEST_JOB_ID,
                "project_id": TEST_GCP_PROJECT_ID,
                "poll_interval": 0.0,
            },
            (BIG_QUERY_TRIGGER_LOGGER, logging.ERROR, "Unknown response from hook: ???"),
            id="error-unknown",
        ),
    ],
    ids=["pending", "success", "error"],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_trigger_lifetime(mock_job_status, caplog, status, event_data, log_record):
    """
    Tests BigQueryTrigger fires during a BigQuery process's lifetime.
    """
    caplog.set_level(logging.DEBUG, logger=BIG_QUERY_TRIGGER_LOGGER)
    mock_job_status.return_value = status

    trigger = BigQueryTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=0.0,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent(event_data) == actual
    assert log_record in caplog.record_tuples


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_op_trigger_error(mock_job_status, caplog):
    """
    Tests BigQueryTrigger fires if the BigQuery process errors.
    """
    caplog.set_level(logging.DEBUG, logger=BIG_QUERY_TRIGGER_LOGGER)
    mock_job_status.side_effect = Exception("I failed!")

    trigger = BigQueryTrigger(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=0.0,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "I failed!"}) == actual
    assert (
        BIG_QUERY_TRIGGER_LOGGER,
        logging.ERROR,
        "Exception occurred while checking for query completion",
    ) in caplog.record_tuples


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

    expected = {
        "status": "success",
        "records": records,
        "job_id": TEST_JOB_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "poll_interval": 0.0,
    }
    assert TriggerEvent(expected) == actual


def test_bigquery_interval_check_trigger_serialization():
    """
    Asserts that the BigQueryIntervalCheckTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryIntervalCheckTrigger(
        TEST_CONN_ID,
        TEST_FIRST_JOB_ID,
        TEST_SECOND_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_TABLE_ID,
        TEST_METRIC_THRESHOLDS,
        TEST_DATE_FILTER_COLUMN,
        TEST_DAYS_BACK,
        TEST_RATIO_FORMULA,
        TEST_IGNORE_ZERO,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger"
    assert kwargs == {
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
    }


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
    assert actual == TriggerEvent({"status": "error", "message": "The second SQL query returned None"})


@pytest.mark.parametrize(
    "trigger_class",
    [BigQueryIntervalCheckTrigger],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_interval_check_trigger_pending(mock_job_status, caplog, trigger_class):
    """
    Tests that the BigQueryIntervalCheckTrigger do not fire while a query is still running.
    """
    mock_job_status.return_value = "pending"
    caplog.set_level(logging.INFO)

    trigger = trigger_class(
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
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False

    assert f"Using the connection  {TEST_CONN_ID} ." in caplog.text

    assert "Query is still running..." in caplog.text
    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.parametrize(
    "trigger_class",
    [BigQueryIntervalCheckTrigger],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_interval_check_trigger_terminated(mock_job_status, trigger_class):
    """
    Tests the BigQueryIntervalCheckTrigger fires the correct event in case of an error.
    """
    # Set the status to a value other than success or pending
    mock_job_status.return_value = "error"
    trigger = trigger_class(
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

    assert TriggerEvent({"status": "error", "message": "error", "data": None}) == actual


@pytest.mark.parametrize(
    "trigger_class",
    [BigQueryIntervalCheckTrigger],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_interval_check_trigger_exception(mock_job_status, caplog, trigger_class):
    """
    Tests that the BigQueryIntervalCheckTrigger fires the correct event in case of an error.
    """
    mock_job_status.side_effect = Exception("Test exception")
    caplog.set_level(logging.DEBUG)

    trigger = trigger_class(
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

    # trigger event is yielded so it creates a generator object
    # so i have used async for to get all the values and added it to task
    task = [i async for i in trigger.run()]
    # since we use return as soon as we yield the trigger event
    # at any given point there should be one trigger event returned to the task
    # so we validate for length of task to be 1

    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


def test_bigquery_value_check_op_trigger_serialization():
    """
    Asserts that the BigQueryValueCheckTrigger correctly serializes its arguments
    and classpath.
    """

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
    classpath, kwargs = trigger.serialize()

    assert classpath == "astronomer.providers.google.cloud.triggers.bigquery.BigQueryValueCheckTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "pass_value": TEST_PASS_VALUE,
        "job_id": TEST_JOB_ID,
        "dataset_id": TEST_DATASET_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "sql": TEST_SQL_QUERY,
        "table_id": TEST_TABLE_ID,
        "tolerance": TEST_TOLERANCE,
        "poll_interval": POLLING_PERIOD_SECONDS,
    }


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
    assert actual == TriggerEvent({"status": "success", "message": "Job completed", "records": [4]})


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_value_check_op_trigger_pending(mock_job_status, caplog):
    """
    Tests that the BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "pending"
    caplog.set_level(logging.INFO)

    trigger = BigQueryValueCheckTrigger(
        TEST_CONN_ID,
        TEST_PASS_VALUE,
        TEST_JOB_ID,
        TEST_DATASET_ID,
        TEST_GCP_PROJECT_ID,
        TEST_SQL_QUERY,
        TEST_TABLE_ID,
        TEST_TOLERANCE,
        POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is False

    assert "Query is still running..." in caplog.text

    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_value_check_op_trigger_fail(mock_job_status):
    """
    Tests that the BigQueryValueCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "dummy"

    trigger = BigQueryValueCheckTrigger(
        TEST_CONN_ID,
        TEST_PASS_VALUE,
        TEST_JOB_ID,
        TEST_DATASET_ID,
        TEST_GCP_PROJECT_ID,
        TEST_SQL_QUERY,
        TEST_TABLE_ID,
        TEST_TOLERANCE,
        POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "error", "message": "dummy", "records": None}) == actual


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_value_check_trigger_exception(mock_job_status):
    """
    Tests the BigQueryValueCheckTrigger does not fire if there is an exception.
    """
    mock_job_status.side_effect = Exception("Test exception")

    trigger = BigQueryValueCheckTrigger(
        conn_id=TEST_CONN_ID,
        sql=TEST_SQL_QUERY,
        pass_value=TEST_PASS_VALUE,
        tolerance=1,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
    )

    # trigger event is yielded so it creates a generator object
    # so i have used async for to get all the values and added it to task
    task = [i async for i in trigger.run()]
    # since we use return as soon as we yield the trigger event
    # at any given point there should be one trigger event returned to the task
    # so we validate for length of task to be 1

    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


def test_big_query_table_existence_trigger_serialization():
    """
    Asserts that the BigQueryTableExistenceTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger"
    assert kwargs == {
        "dataset_id": TEST_DATASET_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "table_id": TEST_TABLE_ID,
        "gcp_conn_id": TEST_GCP_CONN_ID,
        "poke_interval": POLLING_PERIOD_SECONDS,
        "hook_params": TEST_HOOK_PARAMS,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists")
async def test_big_query_table_existence_trigger_success(mock_table_exists):
    """
    Tests success case BigQueryTableExistenceTrigger
    """
    mock_table_exists.return_value = True

    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert TriggerEvent({"status": "success", "message": "success"}) == actual


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists")
async def test_big_query_table_existence_trigger_pending(mock_table_exists):
    """
    Test that BigQueryTableExistenceTrigger is in loop till the table exist.
    """
    mock_table_exists.return_value = False

    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger._table_exists")
async def test_big_query_table_existence_trigger_exception(mock_table_exists):
    """
    Test BigQueryTableExistenceTrigger throws exception if any error.
    """
    mock_table_exists.side_effect = mock.AsyncMock(side_effect=Exception("Test exception"))

    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


@pytest.mark.asyncio
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
async def test_table_exists(mock_get_table_client, mock_get_table_client_value, expected_value):
    """Test BigQueryTableExistenceTrigger._table_exists async function with mocked value and mocked return value"""
    hook = mock.AsyncMock(BigQueryTableHookAsync)
    mock_get_table_client.return_value = mock_get_table_client_value
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    res = await trigger._table_exists(hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID)
    assert res == expected_value


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryTableHookAsync.get_table_client")
async def test_table_exists_exception(mock_get_table_client):
    """Test BigQueryTableExistenceTrigger._table_exists async function with exception and return False"""
    hook = BigQueryTableHookAsync()
    mock_get_table_client.side_effect = ClientResponseError(
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
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    res = await trigger._table_exists(hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID)
    expected_response = False
    assert res == expected_response


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryTableHookAsync.get_table_client")
async def test_table_exists_raise_exception(mock_get_table_client):
    """Test BigQueryTableExistenceTrigger._table_exists async function with raise exception"""
    hook = BigQueryTableHookAsync()
    mock_get_table_client.side_effect = ClientResponseError(
        history=(),
        request_info=RequestInfo(
            headers=CIMultiDict(),
            real_url=URL("https://example.com"),
            method="GET",
            url=URL("https://example.com"),
        ),
        status=400,
        message="Not Found",
    )
    trigger = BigQueryTableExistenceTrigger(
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        TEST_GCP_CONN_ID,
        TEST_HOOK_PARAMS,
        POLLING_PERIOD_SECONDS,
    )
    with pytest.raises(ClientResponseError):
        await trigger._table_exists(hook, TEST_DATASET_ID, TEST_TABLE_ID, TEST_GCP_PROJECT_ID)
