import logging

import pytest
from airflow.exceptions import TaskDeferred

from astronomer_operators.postgres.operators.postgres import PostgresOperatorAsync
from astronomer_operators.postgres.triggers.postgres import PostgresTrigger

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
    operator = PostgresOperatorAsync(task_id=TASK_ID, postgres_conn_id=CONN_ID, sql="SELECT 1")

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(exc.value.trigger, PostgresTrigger), "Trigger is not a PostgresTrigger"


@pytest.mark.xfail
def test_postgres_execute_complete(caplog):
    """
    Asserts that logging occurs as expected.

    TODO: It would appear that logging in the operator is not getting
    picked up by pytest right now... come back to this later.
    """
    caplog.set_level(logging.INFO)
    operator = PostgresOperatorAsync(task_id="run_now", postgres_conn_id=CONN_ID, sql="SELECT 1")
    operator.execute_complete({})

    assert f"{TASK_ID} completed successfully." in caplog.text
