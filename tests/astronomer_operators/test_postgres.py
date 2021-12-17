import asyncio
import logging

import pytest
from airflow.exceptions import TaskDeferred

from astronomer_operators.postgres import PostgresOperatorAsync, PostgresTrigger

TASK_ID = "postgres_check"
CONN_ID = "postgres_default"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_postgres_operator_async(context):
    #     """
    #     Asserts that a task is deferred and a PostgresTrigger will be fired
    #     when the PostgresOperatorAsync is executed.
    #     """
    operator = PostgresOperatorAsync(
        task_id=TASK_ID, postgres_conn_id=CONN_ID, sql="SELECT 1"
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(
        exc.value.trigger, PostgresTrigger
    ), "Trigger is not a PostgresTrigger"


def test_postgres_trigger_serialization():
    """
    Asserts that the PostgresTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = PostgresTrigger(task_id=TASK_ID, sql="SELECT 1", postgres_conn_id=CONN_ID)
    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.postgres.PostgresTrigger"
    assert kwargs == {
        "sql": "SELECT 1",
        "task_id": TASK_ID,
        "postgres_conn_id": CONN_ID,
        "database": None,
        "parameters": None,
        "poll_interval": 1,
    }


@pytest.mark.xfail
def test_postgres_execute_complete(caplog):
    """
    Asserts that logging occurs as expected.

    TODO: It would appear that logging in the operator is not getting
    picked up by pytest right now... come back to this later.
    """
    caplog.set_level(logging.INFO)
    operator = PostgresOperatorAsync(
        task_id="run_now", postgres_conn_id=CONN_ID, sql="SELECT 1"
    )
    operator.execute_complete({})

    assert f"{TASK_ID} completed successfully." in caplog.text


@pytest.mark.asyncio
async def test_postgres_trigger_running(caplog):
    """
    Tests the PostgresTrigger run state
    """
    caplog.set_level(logging.INFO)

    trigger = PostgresTrigger(postgres_conn_id=CONN_ID, task_id=TASK_ID, sql="SELECT 1")

    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(1)

    # TriggerEvent was returned
    assert task.done() is True

    assert "Connecting to postgres" in caplog.text

    # Prevents error when task is destroyed while in "pending" state
    asyncio.get_event_loop().stop()
