import datetime
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.types import DagRunType

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
    context = {}
    yield context


def create_context(task, dag):
    execution_date = datetime.datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
def test_redshiftsql_op_async(mock_execute):
    args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
    dag = DAG("test_redshift_sql_async_execute_complete", default_args=args)
    mock_execute.return_value = [], {}
    task = RedshiftSQLOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        params=TEST_PARAMATERS,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(create_context(task, dag))
    assert isinstance(exc.value.trigger, RedshiftSQLTrigger), "Trigger is not a RedshiftSQLTrigger"


@mock.patch("astronomer.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
def test_redshiftsql_op_async_execute_query_error(mock_execute, context):
    mock_execute.return_value = [], {"status": "error", "message": "Test exception"}
    task = RedshiftSQLOperatorAsync(
        task_id=TEST_TASK_ID,
        sql=TEST_SQL,
        params=TEST_PARAMATERS,
    )
    with pytest.raises(AirflowException):
        task.execute(context)


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
