import datetime
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dag import DAG
from airflow.providers.common.sql.hooks.sql import DbApiHook

from astronomer.providers.snowflake.sensors.snowflake import SnowflakeSensorAsync
from astronomer.providers.snowflake.triggers.snowflake_trigger import SnowflakeSensorTrigger
from tests.utils.airflow_util import create_context

TASK_ID = "snowflake_check"
CONN_ID = "my_snowflake_conn"
RUN_ID = "1"
TASK_TIMEOUT = 1
TEST_SQL = "select * from any;"


class TestPytestSnowflakeSensorAsync:
    @pytest.mark.parametrize("mock_sql", [TEST_SQL, [TEST_SQL]])
    @mock.patch("airflow.providers.common.sql.sensors.sql.BaseHook")
    def test_snowflake_execute_operator_async(self, mock_hook, mock_sql):
        """
        Asserts that a task is deferred and an SnowflakeSensorTrigger will be fired
        when the SnowflakeSensorAsync is executed.
        """
        dag = DAG(dag_id="dag", start_date=datetime.datetime(2023, 1, 1))

        operator = SnowflakeSensorAsync(
            task_id="execute_run",
            snowflake_conn_id=CONN_ID,
            dag=dag,
            sql=mock_sql,
            timeout=TASK_TIMEOUT * 60,
        )

        mock_hook.get_connection.return_value.get_hook.return_value = mock.MagicMock(spec=DbApiHook)

        mock_get_records = mock_hook.get_connection.return_value.get_hook.return_value.get_records

        mock_get_records.return_value = []

        with pytest.raises(TaskDeferred) as exc:
            operator.poke(None)
            operator.execute(create_context(operator, dag=dag))

        assert isinstance(
            exc.value.trigger, SnowflakeSensorTrigger
        ), "Trigger is not a SnowflakeSensorTrigger"

    def test_snowflake_async_execute_complete_failure(self):
        """Tests that an AirflowException is raised in case of error event"""

        operator = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
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

        operator = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
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

        operator = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            timeout=TASK_TIMEOUT * 60,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None)
        mock_log_info.assert_called_with("%s completed successfully.", "execute_complete")
