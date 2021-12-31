import asyncio
import logging
from datetime import timedelta
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer_operators.postgres.triggers.postgres import PostgresTrigger

DAG_ID = "postgres_check_dag"
TASK_ID = "postgres_check"
CONN_ID = "postgres_default"


def test_postgres_trigger_serialization():
    """
    Asserts that the PostgresTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = PostgresTrigger(task_id=TASK_ID, sql="SELECT 1", postgres_conn_id=CONN_ID)
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.postgres.triggers.postgres.PostgresTrigger"
    assert kwargs == {
        "sql": "SELECT 1",
        "task_id": TASK_ID,
        "postgres_conn_id": CONN_ID,
        "application_name": mock.ANY,
        "pid": mock.ANY,
        "poll_interval": 2.0,
    }


@pytest.mark.asyncio
@mock.patch("astronomer_operators.postgres.hooks.postgres.PostgresHookAsync.get_first")
async def test_postgres_trigger_running(mock_get_first, caplog):
    """
    Tests the PostgresTrigger does not fire while a query is still running.
    """
    mock_get_first.return_value = {"state": "active", "age": timedelta(seconds=10)}
    caplog.set_level(logging.DEBUG)

    trigger = PostgresTrigger(
        postgres_conn_id=CONN_ID,
        task_id=TASK_ID,
        application_name=f"{DAG_ID}-{TASK_ID}",
        pid=12345,
        sql="SELECT 1",
        poll_interval=1.0,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is False
    assert "SELECT state, age(clock_timestamp(), query_start) FROM pg_stat_activity" in caplog.text
    assert "application_name = 'postgres_check_dag-postgres_check' and pid = '12345'" in caplog.text

    assert "Query is still running. Time elapsed: 0:00:10. State: active" in caplog.text
    assert "Sleeping for 1.0 seconds." in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer_operators.postgres.hooks.postgres.PostgresHookAsync.get_first")
@pytest.mark.parametrize("state", ["idle", ""])
async def test_postgres_trigger_success(mock_get_first, state):
    """
    Tests the PostgresTrigger only fires once the query execution reaches a successful state.
    """
    mock_get_first.return_value = {"state": state, "age": timedelta(seconds=10)}

    trigger = PostgresTrigger(
        postgres_conn_id=CONN_ID,
        task_id=TASK_ID,
        application_name=f"{DAG_ID}-{TASK_ID}",
        pid=12345,
        sql="SELECT 1",
        poll_interval=1.0,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer_operators.postgres.hooks.postgres.PostgresHookAsync.get_first")
async def test_postgres_trigger_failed(mock_get_first, caplog):
    """
    Tests the PostgresTrigger does not fire if it reaches a failed state.
    """
    mock_get_first.return_value = {"state": "idle in transaction (aborted)"}
    caplog.set_level(logging.DEBUG)

    trigger = PostgresTrigger(
        postgres_conn_id=CONN_ID,
        task_id=TASK_ID,
        application_name=f"{DAG_ID}-{TASK_ID}",
        pid=12345,
        sql="SELECT 1",
        poll_interval=0.5,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    assert task.done() is True
    assert task.result() == TriggerEvent(
        {"status": "error", "message": "One of the statements in the transaction caused an error."}
    )


@pytest.mark.asyncio
@mock.patch("astronomer_operators.postgres.hooks.postgres.PostgresHookAsync.get_first")
async def test_postgres_trigger_exception(mock_get_first, caplog):
    """
    Tests the PostgresTrigger does not fire if there is an exception.
    """
    mock_get_first.side_effect = Exception("Test exception")
    caplog.set_level(logging.DEBUG)

    trigger = PostgresTrigger(
        postgres_conn_id=CONN_ID,
        task_id=TASK_ID,
        application_name=f"{DAG_ID}-{TASK_ID}",
        pid=12345,
        sql="SELECT 1",
        poll_interval=0.5,
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "error", "message": "Test exception"})
