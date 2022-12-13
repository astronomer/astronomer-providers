from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from airflow import AirflowException
from airflow.sensors.base import BaseSensorOperator

from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSensorTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class SnowflakeSensorAsync(BaseSensorOperator):
    """
    Runs a sql statement repeatedly until a criteria is met. It will keep trying until
    success or failure criteria are met, or if the first cell is not in (0, '0', '', None).
    Optional success and failure callables are called with the first cell returned as the argument.
    If success callable is defined the sensor will keep retrying until the criteria is met.
    If failure callable is defined and the criteria is met the sensor will raise AirflowException.
    Failure criteria is evaluated before success criteria. A fail_on_empty boolean can also
    be passed to the sensor in which case it will fail if no rows have been returned
    :param snowflake_conn_id: The connection to run the sensor against
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero / empty string value.
    :param parameters: The parameters to render the SQL query with (optional).
    :param success: Success criteria for the sensor is a Callable that takes first_cell
        as the only argument, and returns a boolean (optional).
    :param failure: Failure criteria for the sensor is a Callable that takes first_cell
        as the only argument and return a boolean (optional).
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
        snowflake_conn_id,
        sql,
        parameters: Optional[Dict] = None,
        success: Optional[Callable[[Any], bool]] = None,
        failure: Optional[Callable[[Any], bool]] = None,
        fail_on_empty: bool = False,
        hook_params: Optional[Dict] = None,
        poll_interval: int = 60,
        **kwargs,
    ):
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.parameters = parameters
        self.success = success
        self.failure = failure
        self.fail_on_empty = fail_on_empty
        self.hook_params = hook_params
        if poll_interval:
            self.poke_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: Context, **kwargs) -> None:
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

    def execute_complete(
        self,
        context: Context,
        event: Optional[Dict[str, Union[str, List[str]]]] = None,
    ) -> Any:
        if event:
            if "status" in event and event["status"] == "error":
                raise AirflowException(event["message"])

            self.log.info(event["message"])
        else:
            self.log.info("%s completed successfully.", self.task_id)
