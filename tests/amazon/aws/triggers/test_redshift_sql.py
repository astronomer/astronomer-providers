from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.redshift_sql import RedshiftSQLTrigger

TEST_CONN_ID = "redshift_default"
TEST_TASK_ID = "123"
POLLING_PERIOD_SECONDS = 4.0


def test_redshift_sql_trigger_serialization():
    """
    Asserts that the RedshiftSQLTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = RedshiftSQLTrigger(
        task_id=TEST_TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id=TEST_CONN_ID,
        query_ids=[],
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.redshift_sql.RedshiftSQLTrigger"
    assert kwargs == {
        "task_id": TEST_TASK_ID,
        "polling_period_seconds": POLLING_PERIOD_SECONDS,
        "aws_conn_id": TEST_CONN_ID,
        "query_ids": [],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_ids,return_value,response",
    [
        (
            ["uuid", "uuid"],
            {"status": "error", "message": "test error"},
            TriggerEvent({"status": "error", "message": "test error"}),
        ),
        (["uuid", "uuid"], False, TriggerEvent({"status": "error", "message": f"{TEST_TASK_ID} failed"})),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.get_query_status")
async def test_redshiftsql_trigger_run(mock_get_query_status, query_ids, return_value, response):
    """
    Tests that RedshiftSQLTrigger only fires once the query execution reaches a successful state.
    """
    mock_get_query_status.return_value = return_value
    trigger = RedshiftSQLTrigger(
        task_id=TEST_TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id=TEST_CONN_ID,
        query_ids=query_ids,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert response in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.get_query_status")
async def test_redshiftsql_trigger_exception(mock_get_query_status, query_ids):
    """
    Test that RedshiftSQLTrigger fires the correct event in case of an error.
    """
    mock_get_query_status.side_effect = Exception("Test exception")

    trigger = RedshiftSQLTrigger(
        task_id=TEST_TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        aws_conn_id=TEST_CONN_ID,
        query_ids=query_ids,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
