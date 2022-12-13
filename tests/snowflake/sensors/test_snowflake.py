import datetime
from datetime import timedelta
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from astronomer.providers.snowflake.sensors.snowflake import SnowflakeSensorAsync
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSensorTrigger,
)

DEFAULT_DATE = timezone.datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]
TEST_DAG_ID = "unit_test_dag"
LONG_MOCK_PATH = "astronomer.providers.snowflake.sensors.snowflake.SnowflakeSensorAsync"
TASK_ID = "snowflake_check"
CONN_ID = "my_snowflake_conn"
RUN_ID = "1"
RUN_PAGE_URL = "https://www.test.com"
RETRY_LIMIT = 2
RETRY_DELAY = 1.0
POLL_INTERVAL = 1.0
TASK_TIMEOUT = 1
XCOM_RUN_ID_KEY = "run_id"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"
TEST_SQL = "select * from any;"

LIFETIME = timedelta(minutes=59)
RENEWAL_DELTA = timedelta(minutes=54)
SQL_MULTIPLE_STMTS = (
    "create or replace table user_test (i int); insert into user_test (i) "
    "values (200); insert into user_test (i) values (300); select i from user_test order by i;"
)

SINGLE_STMT = "select i from user_test order by i;"


@pytest.fixture
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
    return {
        "dag": dag,
        "dag_run": dag_run,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


@pytest.mark.parametrize("mock_sql", [TEST_SQL, [TEST_SQL]])
def test_snowflake_execute_operator_async(mock_sql):
    """
    Asserts that a task is deferred and an SnowflakeSensorTrigger will be fired
    when the SnowflakeSensorAsync is executed.
    """
    args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
    dag = DAG("test_snowflake_async_execute_complete_failure", default_args=args)

    operator = SnowflakeSensorAsync(
        task_id="execute_run",
        snowflake_conn_id=CONN_ID,
        dag=dag,
        sql=mock_sql,
        poll_interval=POLL_INTERVAL,
        timeout=TASK_TIMEOUT * 60,
    )

    with pytest.raises(TaskDeferred) as exc:
        operator.execute(create_context(operator, dag))

    assert isinstance(exc.value.trigger, SnowflakeSensorTrigger), "Trigger is not a SnowflakeSensorTrigger"


def test_snowflake_async_execute_complete_failure():
    """Tests that an AirflowException is raised in case of error event"""

    args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
    dag = DAG("test_snowflake_async_execute_complete_failure", default_args=args)
    operator = SnowflakeSensorAsync(
        task_id="execute_complete",
        snowflake_conn_id=CONN_ID,
        dag=dag,
        sql=TEST_SQL,
        poll_interval=POLL_INTERVAL,
        timeout=TASK_TIMEOUT * 60,
    )
    with pytest.raises(AirflowException):
        operator.execute_complete(
            context=None,
            event={
                "status": "error",
                "message": "Test failure message",
                "type": "FAILED_WITH_ERROR",
            },
        )


@pytest.mark.parametrize(
    "mock_event",
    [
        {"status": "success", "message": "Found expected markers."},
        {"status": "success", "message": "Found expected markers."},
    ],
)
def test_snowflake_async_execute_complete(mock_event):
    """Tests execute_complete assert with successful message"""

    args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
    dag = DAG("test_snowflake_async_execute_complete", default_args=args)
    operator = SnowflakeSensorAsync(
        task_id="execute_complete",
        snowflake_conn_id=CONN_ID,
        sql=TEST_SQL,
        dag=dag,
        poll_interval=POLL_INTERVAL,
        timeout=TASK_TIMEOUT * 60,
    )

    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=None, event=mock_event)
    mock_log_info.assert_called_with("Found expected markers.")
