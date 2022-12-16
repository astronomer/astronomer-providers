import datetime
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.types import DagRunType

from astronomer.providers.snowflake.sensors.snowflake import SnowflakeSensorAsync
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSensorTrigger,
)

TASK_ID = "snowflake_check"
CONN_ID = "my_snowflake_conn"
RUN_ID = "1"
TASK_TIMEOUT = 1
TEST_SQL = "select * from any;"


class TestPytestSnowflakeSensorAsync:
    @pytest.fixture
    def context(self):
        """
        Creates an empty context.
        """
        context = {}
        yield context

    def create_context(self, task, dag):
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
    def test_snowflake_execute_operator_async(self, mock_sql):
        """
        Asserts that a task is deferred and an SnowflakeSensorTrigger will be fired
        when the SnowflakeSensorAsync is executed.
        """
        args = {
            "owner": "airflow",
            "start_date": datetime.datetime(2017, 1, 1),
        }
        dag = DAG("test_snowflake_async_execute_complete_failure", default_args=args)

        operator = SnowflakeSensorAsync(
            task_id="execute_run",
            snowflake_conn_id=CONN_ID,
            dag=dag,
            sql=mock_sql,
            timeout=TASK_TIMEOUT * 60,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(self.create_context(operator, dag))

        assert isinstance(
            exc.value.trigger, SnowflakeSensorTrigger
        ), "Trigger is not a SnowflakeSensorTrigger"

    def test_snowflake_async_execute_complete_failure(self):
        """Tests that an AirflowException is raised in case of error event"""

        args = {
            "owner": "airflow",
            "start_date": datetime.datetime(2017, 1, 1),
        }
        dag = DAG("test_snowflake_async_execute_complete_failure", default_args=args)
        operator = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            dag=dag,
            sql=TEST_SQL,
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
    def test_snowflake_async_execute_complete(self, mock_event):
        """Tests execute_complete assert with successful message"""

        args = {
            "owner": "airflow",
            "start_date": datetime.datetime(2017, 1, 1),
        }
        dag = DAG("test_snowflake_async_execute_complete", default_args=args)
        operator = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            dag=dag,
            timeout=TASK_TIMEOUT * 60,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("Found expected markers.")

    def test_snowflake_async_execute_complete_no_event(self):
        """
        Tests execute_complete assert with successful message without
        event marker.
        """

        args = {
            "owner": "airflow",
            "start_date": datetime.datetime(2017, 1, 1),
        }
        dag = DAG("test_snowflake_async_execute_complete", default_args=args)
        operator = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            dag=dag,
            timeout=TASK_TIMEOUT * 60,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None)
        mock_log_info.assert_called_with("%s completed successfully.", "execute_complete")
