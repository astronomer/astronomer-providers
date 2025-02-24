import datetime
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException, TaskDeferred
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


def dummy_callable(result):
    return True


def dummy_callable_false(result):
    return False


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
            None,
            {"status": "success", "message": "Found expected markers."},
        ],
    )
    @mock.patch("astronomer.providers.snowflake.sensors.snowflake.raise_error_or_skip_exception")
    def test_snowflake_async_execute_complete(self, mock_error_skip, mock_event):
        """Tests execute_complete assert with successful message"""

        sensor = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            timeout=TASK_TIMEOUT * 60,
        )

        with mock.patch.object(sensor.log, "info") as mock_log_info:
            sensor.execute_complete(context=None, event=mock_event)
        if mock_event:
            mock_log_info.assert_called_with("Found expected markers.")
        else:
            mock_error_skip.assert_called_once_with(sensor.soft_fail, "Trigger returns an empty event")

    def test_execute_complete_validate(self):
        sensor = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            timeout=TASK_TIMEOUT * 60,
            success=dummy_callable,
        )

        result = sensor.execute_complete(context=None, event={"status": "validate", "result": [[True]]})
        assert result is None

    @mock.patch("astronomer.providers.snowflake.sensors.snowflake.SnowflakeSensorAsync._defer")
    def test_execute_complete_validate_false(self, mock_defer):
        sensor = SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            timeout=TASK_TIMEOUT * 60,
            success=dummy_callable_false,
        )

        sensor.execute_complete(context=None, event={"status": "validate", "result": [[True]], "message": ""})
        mock_defer.assert_called_once()

    def test_soft_fail_enable(self, context):
        """Sensor should raise AirflowSkipException if soft_fail is True and error occur"""
        sensor = SnowflakeSensorAsync(
            task_id="snowflake_sensor",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            timeout=TASK_TIMEOUT * 60,
            soft_fail=True,
        )
        with pytest.raises(AirflowSkipException):
            sensor.execute(context)

    @mock.patch("airflow.providers.common.sql.sensors.sql.SqlSensor.__init__")
    def test_call_with_parameters(self, mock_sql_sensor):
        """Ensure SQL parameters are passed to the SqlSensor parent class."""
        SnowflakeSensorAsync(
            sql="SELECT * FROM %(src)s;",
            task_id="snowflake_sensor",
            snowflake_conn_id=CONN_ID,
            parameters={"src": "my_table"},
        )

        mock_sql_sensor.assert_called_once_with(
            conn_id=CONN_ID,
            task_id="snowflake_sensor",
            sql="SELECT * FROM %(src)s;",
            parameters={"src": "my_table"},
            success=None,
            failure=None,
            fail_on_empty=False,
            hook_params=None,
            default_args={},
        )

    @staticmethod
    def get_sensor_instance():
        return SnowflakeSensorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            timeout=TASK_TIMEOUT * 60,
        )

    def test_validate_result_empty_result(self):
        instance = self.get_sensor_instance()
        instance.fail_on_empty = True
        with pytest.raises(AirflowException) as exc_info:
            instance._validate_result([])
        assert "No rows returned, raising as per fail_on_empty flag" in str(exc_info.value)

    def test_validate_result_fail_empty_false(self):
        instance = self.get_sensor_instance()
        instance.fail_on_empty = False
        result = instance._validate_result([])
        assert not result

    def test_validate_result(self):
        instance = self.get_sensor_instance()
        result = instance._validate_result([("cell_value",)])
        assert result

    def test_validate_result_failure(self):
        instance = self.get_sensor_instance()
        instance.failure = "failed"
        with pytest.raises(AirflowException) as exc_info:
            instance._validate_result([("",)])
        assert "self.failure is present, but not callable -> failed" in str(exc_info.value)

    def test_validate_result_failure_callable(self):
        instance = self.get_sensor_instance()
        instance.failure = dummy_callable
        with pytest.raises(AirflowException) as exc_info:
            instance._validate_result([("1", "2")])
        assert "Failure criteria met. self.failure(1) returned True" in str(exc_info.value)

    def test_validate_result_success(self):
        instance = self.get_sensor_instance()
        instance.success = "success"
        with pytest.raises(AirflowException) as exc_info:
            instance._validate_result([("cell_value",)])
        assert "self.success is present, but not callable ->" in str(exc_info.value)

    def test_validate_result_success_callable(self):
        instance = self.get_sensor_instance()
        instance.success = dummy_callable

        result = instance._validate_result([("cell_value",)])
        assert result
