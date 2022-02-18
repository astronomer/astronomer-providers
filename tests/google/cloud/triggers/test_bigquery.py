import asyncio
import logging
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.google.cloud.triggers.bigquery import BigQueryInsertJobTrigger

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


def test_bigquery_trigger_serialization():
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
async def test_bigquery_trigger_success(mock_job_status):
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


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_trigger_running(mock_job_status, caplog):
    """
    Tests the BigQueryInsertJobTrigger does not fire while a query is still running.
    """
    mock_job_status.return_value = "pending"
    caplog.set_level(logging.INFO)

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

    # TriggerEvent was not returned
    assert task.done() is False

    assert f"Using the connection  {TEST_CONN_ID} ." in caplog.text

    assert "Query is still running..." in caplog.text
    assert f"Sleeping for {POLLING_PERIOD_SECONDS} seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_trigger_terminated(mock_job_status):
    """
    Tests the BigQueryInsertJobTrigger does not fire if it reaches a failed state.
    """
    # Set the status to a value other than success or pending
    mock_job_status.return_value = "error"

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

    assert task.result() == TriggerEvent({"status": "error", "message": "error"})

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.google.cloud.hooks.bigquery.BigQueryHookAsync.get_job_status")
async def test_bigquery_trigger_exception(mock_job_status, caplog):
    """
    Tests the BigQueryInsertJobTrigger does not fire if there is an exception.
    """
    mock_job_status.side_effect = Exception("Test exception")
    caplog.set_level(logging.DEBUG)

    trigger = BigQueryInsertJobTrigger(
        TEST_CONN_ID,
        TEST_JOB_ID,
        TEST_GCP_PROJECT_ID,
        TEST_DATASET_ID,
        TEST_TABLE_ID,
        POLLING_PERIOD_SECONDS,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "error", "message": "Test exception"})
