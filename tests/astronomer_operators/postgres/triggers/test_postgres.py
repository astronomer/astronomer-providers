import asyncio
import logging
from unittest import mock

import pytest

from astronomer_operators.postgres.triggers.postgres import PostgresTrigger

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
async def test_postgres_trigger_running(caplog):
    """
    Tests the PostgresTrigger run state
    """
    caplog.set_level(logging.DEBUG)

    trigger = PostgresTrigger(postgres_conn_id=CONN_ID, task_id=TASK_ID, sql="SELECT 1")

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    # TriggerEvent was returned
    print("#######", task.done())
    assert task.done() is True
    print("$$$$$$$", caplog.text)
    assert "Connecting to localhost" in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()
