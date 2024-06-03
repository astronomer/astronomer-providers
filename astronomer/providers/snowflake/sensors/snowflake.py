from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.sensors.sql import SqlSensor

from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSensorTrigger,
)
from astronomer.providers.utils.sensor_util import poke, raise_error_or_skip_exception
from astronomer.providers.utils.typing_compat import Context


class SnowflakeSensorAsync(SqlSensor):
    """
    Runs a sql statement repeatedly until a criteria is met. It will keep trying until
    success or failure criteria are met, or if the first cell returned from the query
    is not in (0, '0', '', None).
    Optional success and failure callables are called with the first cell returned
    from the query as the argument.
    If success callable is defined the sensor will keep retrying until the criteria is met.
    If failure callable is defined and the criteria is met the sensor will raise AirflowException.
    Failure criteria is evaluated before success criteria. A fail_on_empty boolean can also
    be passed to the sensor in which case it will fail if no rows have been returned.

    :param snowflake_conn_id: The connection to run the sensor against
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    :param parameters: The parameters to render the SQL query with (optional).
    :param success: Success criteria for the sensor is a Callable that takes the first cell
        returned from the query as the only argument, and returns a boolean (optional).
    :param failure: Failure criteria for the sensor is a Callable that takes the first cell
        returned from the query as the only argument and return a boolean (optional).
    :param fail_on_empty: Explicitly fail on no rows returned.
    :param hook_params: Extra config params to be passed to the underlying hook.
        Should match the desired hook constructor params.
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#7c7287"

    def __init__(
        self,
        *,
        snowflake_conn_id: str,
        sql: str,
        parameters: dict[str, Any] | None = None,
        success: Callable[[Any], bool] | None = None,
        failure: Callable[[Any], bool] | None = None,
        fail_on_empty: bool = False,
        hook_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.parameters = parameters
        self.success = success
        self.failure = failure
        self.fail_on_empty = fail_on_empty
        self.hook_params = hook_params
        super().__init__(
            conn_id=snowflake_conn_id,
            sql=sql,
            parameters=parameters,
            success=success,
            failure=failure,
            fail_on_empty=fail_on_empty,
            hook_params=hook_params,
            **kwargs,
        )

    def execute(self, context: Context) -> None:
        """Check for query result in Snowflake by deferring using the trigger"""
        if not poke(self, context):
            self._defer(context)

    def _validate_result(self, result: list[tuple[Any]]) -> Any:
        """Validates query result and verifies if it returns a row"""
        if not result:
            if self.fail_on_empty:
                raise AirflowException("No rows returned, raising as per fail_on_empty flag")
            else:
                return False

        first_cell = result[0][0]
        if self.failure is not None:
            if callable(self.failure):
                if self.failure(first_cell):
                    raise AirflowException(f"Failure criteria met. self.failure({first_cell}) returned True")
            else:
                raise AirflowException(f"self.failure is present, but not callable -> {self.failure}")
        if self.success is not None:
            if callable(self.success):
                return self.success(first_cell)
            else:
                raise AirflowException(f"self.success is present, but not callable -> {self.success}")
        return bool(first_cell)

    def _defer(self, context: Context) -> None:
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=SnowflakeSensorTrigger(
                sql=self.sql,
                poke_interval=self.poke_interval,
                parameters=self.parameters,
                success=self.success,
                failure=self.failure,
                fail_on_empty=self.fail_on_empty,
                dag_id=context["dag"].dag_id,
                task_id=context["task"].task_id,
                run_id=context["dag_run"].run_id,
                snowflake_conn_id=self.snowflake_conn_id,
            ),
            method_name=self.execute_complete.__name__,
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "validate":
                if self._validate_result(event["result"]):
                    self.log.info("%s completed successfully.", self.task_id)
                    return
                else:
                    self._defer(context)
            if "status" in event and event["status"] == "error":
                raise_error_or_skip_exception(self.soft_fail, event["message"])
            self.log.info(event["message"])
        else:
            raise_error_or_skip_exception(self.soft_fail, "Trigger returns an empty event")
