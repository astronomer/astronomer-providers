import asyncio
import logging
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer_operators.snowflake.triggers.snowflake_trigger import SnowflakeTrigger
from tests.astronomer_operators.snowflake.operators.test_snowflake_async import (
    POLLING_PERIOD_SECONDS,
    TASK_ID,
)


def test_snowflake_trigger_serialization():
    """
    Asserts that the SnowflakeTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = SnowflakeTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        query_ids=[],
        snowflake_conn_id="test_conn",
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.snowflake.triggers.snowflake_trigger.SnowflakeTrigger"
    assert kwargs == {
        "task_id": TASK_ID,
        "polling_period_seconds": 1.0,
        "query_ids": [],
        "snowflake_conn_id": "test_conn",
    }


@pytest.mark.asyncio
@mock.patch("astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
async def test_snowflake_trigger_running(mock_get_first, query_ids):
    """
    Tests that the SnowflakeTrigger in
    """
    trigger = SnowflakeTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        query_ids=query_ids,
        snowflake_conn_id="test_conn",
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
async def test_snowflake_trigger_success(mock_get_first, query_ids):
    """
    Tests that the SnowflakeTrigger in success case
    """
    mock_get_first.return_value = {"status": "success", "query_ids": query_ids}

    trigger = SnowflakeTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        query_ids=query_ids,
        snowflake_conn_id="test_conn",
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was returned
    assert task.done() is True
    print(task.result())
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
async def test_snowflake_trigger_failed(mock_get_first, query_ids):
    """
    Tests the SnowflakeTrigger does not fire if it reaches a failed state.
    """
    mock_get_first.return_value = {
        "status": "error",
        "message": "The query is in the process of being aborted on the server side.",
        "type": "ABORTING",
    }

    trigger = SnowflakeTrigger(
        task_id=TASK_ID,
        polling_period_seconds=0.5,
        query_ids=query_ids,
        snowflake_conn_id="test_conn",
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    # TriggerEvent was returned
    assert task.done() is True
    assert task.result() == TriggerEvent(
        {
            "status": "error",
            "message": "The query is in the process of being aborted on the server side.",
            "type": "ABORTING",
        }
    )


@pytest.mark.asyncio
@mock.patch("astronomer_operators.snowflake.hooks.snowflake.SnowflakeHookAsync.get_conn")
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
async def test_snowflake_trigger_exception(mock_conn, caplog, query_ids):
    """
    Tests the SnowflkeTrigger does not fire if there is an exception.
    """
    mock_conn.return_value = mock.MagicMock(
        is_still_running=mock.MagicMock(side_effect=Exception("Test exception"))
    )

    caplog.set_level(logging.DEBUG)
    trigger = SnowflakeTrigger(
        task_id=TASK_ID,
        polling_period_seconds=0.5,
        query_ids=query_ids,
        snowflake_conn_id="test_conn",
    )

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    assert task.done() is True
    assert task.result() == TriggerEvent({"status": "error", "message": "Test exception", "type": "ERROR"})
