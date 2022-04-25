import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.snowflake.triggers.snowflake_trigger import SnowflakeTrigger

TASK_ID = "snowflake_check"
POLLING_PERIOD_SECONDS = 1.0


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
    assert classpath == "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeTrigger"
    assert kwargs == {
        "task_id": TASK_ID,
        "polling_period_seconds": 1.0,
        "query_ids": [],
        "snowflake_conn_id": "test_conn",
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
        (
            ["uuid", "uuid"],
            {"status": "success", "query_ids": ["uuid", "uuid"]},
            TriggerEvent({"status": "success", "query_ids": ["uuid", "uuid"]}),
        ),
        (
            ["uuid", "uuid"],
            False,
            TriggerEvent(
                {
                    "status": "error",
                    "message": f"{TASK_ID} " f"failed with terminal state: False",
                }
            ),
        ),
    ],
)
@mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
async def test_snowflake_trigger_running(mock_get_query_status, query_ids, return_value, response):
    """Tests that the SnowflakeTrigger in"""
    mock_get_query_status.return_value = return_value
    trigger = SnowflakeTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        query_ids=query_ids,
        snowflake_conn_id="test_conn",
    )

    generator = trigger.run()
    actual = await generator.asend(None)
    assert response == actual


@pytest.mark.asyncio
@mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
async def test_snowflake_trigger_success(mock_get_first, query_ids):
    """Tests that the SnowflakeTrigger in success case"""
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
    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
async def test_snowflake_trigger_failed(mock_get_first, query_ids):
    """Tests the SnowflakeTrigger does not fire if it reaches a failed state."""
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
@pytest.mark.parametrize(
    "query_ids",
    [
        (["uuid", "uuid"]),
    ],
)
@mock.patch("astronomer.providers.snowflake.hooks.snowflake.SnowflakeHookAsync.get_query_status")
async def test_snowflake_trigger_exception(mock_query_status, query_ids):
    """Tests the SnowflakeTrigger does not fire if there is an exception."""
    mock_query_status.side_effect = Exception("Test exception")

    trigger = SnowflakeTrigger(
        task_id=TASK_ID,
        polling_period_seconds=0.5,
        query_ids=query_ids,
        snowflake_conn_id="test_conn",
    )

    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
