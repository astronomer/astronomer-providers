from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.amazon.aws.triggers.redshift_sql import RedshiftSQLTrigger

TEST_CONN_ID = "redshift_default"
TEST_TASK_ID = "123"
POLLING_PERIOD_SECONDS = 4.0
TEST_SQL = "select * from any"
TEST_PARAMATERS = {}


def test_redhsift_sqltrigger_serialization():
    """
    Asserts that the RedshiftSQLTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = RedshiftSQLTrigger(
        task_id=TEST_TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        redshift_conn_id=TEST_CONN_ID,
        sql=TEST_SQL,
        parameters=TEST_PARAMATERS,
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.amazon.aws.triggers.redshift_sql.RedshiftSQLTrigger"
    assert kwargs == {
        "task_id": TEST_TASK_ID,
        "polling_period_seconds": POLLING_PERIOD_SECONDS,
        "redshift_conn_id": TEST_CONN_ID,
        "sql": TEST_SQL,
        "parameters": TEST_PARAMATERS,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "return_value,response",
    [
        (
            {"status": "error", "message": "test error"},
            TriggerEvent({"status": "error", "message": "test error"}),
        ),
        (False, TriggerEvent({"status": "error", "message": f"{TEST_TASK_ID} failed"})),
    ],
)
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.execute_query")
async def test_redshiftsql_trigger_run(mock_execute_query, return_value, response):
    """
    Tests that RedshiftSQLTrigger only fires once the query execution reaches a successful state.
    """
    mock_execute_query.return_value = return_value
    trigger = RedshiftSQLTrigger(
        task_id=TEST_TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        redshift_conn_id=TEST_CONN_ID,
        sql=TEST_SQL,
        parameters=TEST_PARAMATERS,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert response in task


@pytest.mark.asyncio
@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHookAsync.execute_query")
async def test_redshiftsql_trigger_exception(mock_execute_query):
    """
    Test that RedshiftSQLTrigger fires the correct event in case of an error.
    """
    mock_execute_query.side_effect = Exception("Test exception")

    trigger = RedshiftSQLTrigger(
        task_id=TEST_TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        redshift_conn_id=TEST_CONN_ID,
        sql=TEST_SQL,
        parameters=TEST_PARAMATERS,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
