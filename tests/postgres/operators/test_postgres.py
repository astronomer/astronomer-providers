from unittest import mock

import pytest
from airflow.exceptions import AirflowException, AirflowNotFoundException, TaskDeferred

from astronomer_operators.postgres.operators.postgres import PostgresOperatorAsync
from astronomer_operators.postgres.triggers.postgres import PostgresTrigger

TASK_ID = "postgres_check"
CONN_ID = "postgres_conn_id"
DAG_ID = "postgres_check_dag"


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_postgres_operator_async_missing_connection(context):
    """
    Checks that AirflowNotFoundException is raised when there is no connection available
    """
    op = PostgresOperatorAsync(task_id=TASK_ID, postgres_conn_id=CONN_ID, sql="SELECT 1")

    with pytest.raises(AirflowNotFoundException) as exc:
        op.execute(context)

    assert exc.value.title == f"The conn_id `{CONN_ID}` isn't defined"


@mock.patch("astronomer_operators.postgres.operators.postgres._PostgresHook.run")
def test_postgres_operator_async(run_response, context):
    """
    Asserts that a task is deferred and a PostgresTrigger will be fired
    when the PostgresOperatorAsync is executed.
    """
    run_response.return_value = 12345
    operator = PostgresOperatorAsync(task_id=TASK_ID, postgres_conn_id=CONN_ID, sql="SELECT 1")

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)

    assert isinstance(exc.value.trigger, PostgresTrigger), "Trigger is not a PostgresTrigger"


def test_postgres_execute_complete():
    """Asserts that logging occurs as expected"""
    operator = PostgresOperatorAsync(task_id="run_now", postgres_conn_id=CONN_ID, sql="SELECT 1")
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(
            context=None, event={"status": "success", "message": "Query Execution completed. "}
        )

    mock_log_info.assert_called_with(
        "%s completed successfully with response %s ", "run_now", "Query Execution completed. "
    )


def test_postgres_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    operator = PostgresOperatorAsync(task_id="run_now", postgres_conn_id=CONN_ID, sql="SELECT 1")

    with pytest.raises(AirflowException):
        operator.execute_complete(context=None, event={"status": "error", "message": "test failure message"})
