from __future__ import annotations

import datetime
import warnings
from typing import TYPE_CHECKING, Any

from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session

from astronomer.providers.core.triggers.external_task import (
    DagStateTrigger,
    ExternalDeploymentTaskTrigger,
    TaskStateTrigger,
)
from astronomer.providers.utils.sensor_util import poke, raise_error_or_skip_exception
from astronomer.providers.utils.typing_compat import Context

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class ExternalTaskSensorAsync(ExternalTaskSensor):  # noqa: D101
    def __init__(
        self,
        poke_interval: float = 5.0,
        **kwargs: Any,
    ) -> None:
        warnings.warn(
            (
                "This module is deprecated and will be removed in airflow>=2.9.0"
                "Please use `airflow.sensors.external_task.ExternalTaskSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**kwargs)
        self.poke_interval = poke_interval

    def execute(self, context: Context) -> None:
        """Correctly identify which trigger to execute, and defer execution as expected."""
        execution_dates = self.get_execution_dates(context)

        # Work out if we are a DAG sensor or a Task sensor
        # Defer to our trigger
        if not poke(self, context):
            if (
                not self.external_task_id
            ):  # Tempting to explicitly check for None, but this captures falsely values
                self.defer(
                    timeout=datetime.timedelta(seconds=self.timeout),
                    trigger=DagStateTrigger(
                        dag_id=self.external_dag_id,
                        # The trigger does not do pass/fail, only "a state was reached",
                        # so we pass it all states that might make us pass or fail, and
                        # then work out which result we have in execute_complete.
                        states=self.allowed_states + self.failed_states,
                        execution_dates=execution_dates,
                        poll_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )
            else:
                self.defer(
                    timeout=datetime.timedelta(seconds=self.timeout),
                    trigger=TaskStateTrigger(
                        dag_id=self.external_dag_id,
                        task_id=self.external_task_id,
                        states=self.allowed_states + self.failed_states,
                        execution_dates=execution_dates,
                        poll_interval=self.poke_interval,
                    ),
                    method_name="execute_complete",
                )

    @provide_session
    def execute_complete(  # type: ignore[override]
        self, context: Context, session: Session, event: dict[str, Any] | None = None
    ) -> None:
        """Verifies that there is a success status for each task via execution date."""
        execution_dates = self.get_execution_dates(context)
        count_allowed = self.get_count(execution_dates, session, self.allowed_states)
        if count_allowed != len(execution_dates):
            if self.external_task_id:
                error = f"The external task {self.external_task_id} in DAG {self.external_dag_id} failed."
            else:
                error = f"The external DAG {self.external_dag_id} failed."
            raise_error_or_skip_exception(self.soft_fail, error)
        return None

    def get_execution_dates(self, context: Context) -> list[datetime.datetime]:
        """Helper function to set execution dates depending on which context and/or internal fields are populated."""
        if self.execution_delta:
            execution_date: datetime.datetime = context["execution_date"] - self.execution_delta
        elif self.execution_date_fn:
            execution_date = self._handle_execution_date_fn(context=context)
        else:
            execution_date = context["execution_date"]
        execution_dates = execution_date if isinstance(execution_date, list) else [execution_date]
        return execution_dates


class ExternalDeploymentTaskSensorAsync(HttpSensor):
    """
    External deployment task sensor Make HTTP call and poll for the response state of externally
    deployed DAG task to complete. Inherits from HttpSensor, the host should be external deployment url, header
    with access token

    .. seealso::
        - `Retrieve an access token and Deployment URL <https://docs.astronomer.io/astro/airflow-api#step-1-retrieve-an-access-token-and-deployment-url.>`_

    :param http_conn_id: The Connection ID to run the sensor against
    :param method: The HTTP request method to use
    :param endpoint: The relative part of the full url
    :param request_params: The parameters to be added to the GET url
    :param headers: The HTTP headers to be added to the GET request
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    :param poke_interval: Time in seconds that the job should wait in between each tries
    """  # noqa

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=self.execution_timeout,
            trigger=ExternalDeploymentTaskTrigger(
                http_conn_id=self.http_conn_id,
                method=self.method,
                endpoint=self.endpoint,
                data=self.request_params,
                headers=self.headers,
                extra_options=self.extra_options,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Return true and log the response if state is not success state raise ValueError
        """
        if event and "state" in event:
            if event["state"] == "success":
                self.log.info("Task Succeeded with response: %s", event)
                return True
        raise ValueError(f"Task Failed with response: {event}")
