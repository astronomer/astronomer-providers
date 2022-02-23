import asyncio
import logging
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryCheckTrigger,
    BigQueryInsertJobTrigger,
)

TEST_CONN_ID = "bq_default"
TEST_JOB_ID = "1234"
RUN_ID = "1"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLLING_PERIOD_SECONDS = 1.0
TEST_GCP_PROJECT_ID = "test-project"
TEST_DATASET_ID = "bq_dataset"
TEST_TABLE_ID = "bq_table"
POLLING_PERIOD_SECONDS = 4.0


def test_bigquery_insert_job_op_trigger_serialization():
    """
    Asserts that the BigQueryInsertJobTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryInsertJobTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "job_id": TEST_JOB_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "dataset_id": TEST_DATASET_ID,
        "table_id": TEST_TABLE_ID,
        "poll_interval": POLLING_PERIOD_SECONDS,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_insert_job_op_trigger_success(mock_job_status):
    """
    Tests the BigQueryInsertJobTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"

    trigger = BigQueryInsertJobTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.parametrize(
    "trigger_class",
    [BigQueryInsertJobTrigger, BigQueryCheckTrigger],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_op_trigger_running(mock_job_status, caplog, trigger_class):
    """
    Tests that the  BigQueryInsertJobTrigger and BigQueryCheckTrigger do not fire while a query is still running.
    """
    mock_job_status.return_value = "pending"
    caplog.set_level(logging.INFO)

    trigger = trigger_class(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
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
    [BigQueryInsertJobTrigger, BigQueryCheckTrigger],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_op_trigger_terminated(mock_job_status, trigger_class):
    """
    Tests the BigQueryInsertJobTrigger and BigQueryCheckTrigger fires the correct event in case of an error.
    """
    # Set the status to a value other than success or pending
    mock_job_status.return_value = "error"

    trigger = trigger_class(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True

    assert task.result() == TriggerEvent({"status": "error", "message": "error"})

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.parametrize(
    "trigger_class",
    [BigQueryInsertJobTrigger, BigQueryCheckTrigger],
)
@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_op_trigger_exception(mock_job_status, caplog, trigger_class):
    """
    Tests that the BigQueryInsertJobTrigger and BigQueryCheckTrigger fires the correct event in case of an error.
    """
    mock_job_status.side_effect = Exception("Test exception")
    caplog.set_level(logging.DEBUG)

    trigger = trigger_class(
        conn_id=TEST_CONN_ID,
        job_id=TEST_JOB_ID,
        project_id=TEST_GCP_PROJECT_ID,
        dataset_id=TEST_DATASET_ID,
        table_id=TEST_TABLE_ID,
        poll_interval=POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "error", "message": "Test exception"})


def test_bigquery_check_op_trigger_serialization():
    """
    Asserts that the BigQueryCheckTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = BigQueryCheckTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.bigquery.BigQueryCheckTrigger"
    assert kwargs == {
        "conn_id": TEST_CONN_ID,
        "job_id": TEST_JOB_ID,
        "dataset_id": TEST_DATASET_ID,
        "project_id": TEST_GCP_PROJECT_ID,
        "table_id": TEST_TABLE_ID,
        "poll_interval": POLLING_PERIOD_SECONDS,
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_output")
async def test_bigquery_check_op_trigger_success_with_data(mock_job_output, mock_job_status):
    """
    Tests that the BigQueryCheckTrigger only fires once the query execution reaches a successful state.
    """
    mock_job_status.return_value = "success"
    mock_job_output.return_value = {
        "kind": "bigquery#getQueryResultsResponse",
        "etag": "test_etag",
        "schema": {"fields": [{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"}]},
        "jobReference": {
            "projectId": "test_astronomer-airflow-providers",
            "jobId": "test_jobid",
            "location": "US",
        },
        "totalRows": "1",
        "rows": [{"f": [{"v": "22"}]}],
        "totalBytesProcessed": "0",
        "jobComplete": True,
        "cacheHit": False,
    }

    trigger = BigQueryCheckTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True

    # The extracted row will be parsed and formatted to retrieve the value [22] from the
    # structure - 'rows': [{'f': [{'v': '22'}]}]

    assert task.result() == TriggerEvent({"status": "success", "records": [22]})

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_output")
async def test_bigquery_check_op_trigger_success_without_data(mock_job_output, mock_job_status):
    """
    Tests that the BigQueryCheckTrigger sends TriggerEvent as  { "status": "success", "records": None}
    when no rows are available in the query result.
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
        "totalRows": "0",
        "totalBytesProcessed": "0",
        "jobComplete": True,
        "cacheHit": False,
    }

    trigger = BigQueryCheckTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "success", "records": None})

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()
