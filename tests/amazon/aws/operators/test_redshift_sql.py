from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.operators.redshift_sql import (
    RedshiftSQLOperatorAsync,
)
from astronomer.providers.amazon.aws.triggers.redshift_sql import RedshiftSQLTrigger

TEST_TASK_ID = "123"
TEST_SQL = "select * from any"
TEST_PARAMATERS = {}


@pytest.fixture(scope="function")
def context():
    """
    Creates an empty context.
    """
    yield


@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
def test_redshiftsql_op_async(mock_execute):
    mock_execute.return_value = [], {}
    task = RedshiftSQLOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        params=TEST_PARAMATERS,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, RedshiftSQLTrigger), "Trigger is not a RedshiftSQLTrigger"


def test_redshiftsql_op_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""

    task = RedshiftSQLOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        params=TEST_PARAMATERS,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


@pytest.mark.parametrize(
    "event",
    [None, {"status": "success", "message": "Job completed"}],
)
def test_redshiftsql_op_async_execute_complete(event):
    """Asserts that logging occurs as expected"""
    task = RedshiftSQLOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        params=TEST_PARAMATERS,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event=event)
    mock_log_info.assert_called_with("%s completed successfully.", TEST_TASK_ID)
