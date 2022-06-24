from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.operators.redshift_data import (
    RedshiftDataOperatorAsync,
)
from astronomer.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger

TEST_DATABASE = "TEST_DATABASE"
TEST_PARAMETERS = {}
TEST_TASK_ID = "123"
TEST_SQL = "select * from any"


@pytest.fixture(scope="function")
def context():
    """
    Creates an empty context.
    """
    yield


@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
def test_redshift_data_op_async(mock_execute):
    mock_execute.return_value = [], {}
    task = RedshiftDataOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        database=TEST_DATABASE,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, RedshiftDataTrigger), "Trigger is not a RedshiftDataTrigger"


@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
def test_redshift_data_op_async_execute_query_error(mock_execute, context):
    mock_execute.return_value = [], {"status": "error", "message": "Test exception"}
    task = RedshiftDataOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        database=TEST_DATABASE,
    )
    with pytest.raises(AirflowException):
        task.execute(context)


def test_redshift_data_op_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""

    task = RedshiftDataOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        database=TEST_DATABASE,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


@pytest.mark.parametrize(
    "event",
    [None, {"status": "success", "message": "Job completed"}],
)
def test_redshift_data_op_async_execute_complete(event):
    """Asserts that logging occurs as expected"""
    task = RedshiftDataOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        database=TEST_DATABASE,
    )
    if not event:
        with pytest.raises(AirflowException) as exception_info:
            task.execute_complete(context=None, event=None)
        assert exception_info.value.args[0] == "Did not receive valid event from the trigerrer"
    else:
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event=event)
        mock_log_info.assert_called_with("%s completed successfully.", TEST_TASK_ID)
