import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.context import Context
from airflow.utils.session import provide_session

from astronomer.providers.core.triggers.external_task import (
    DagStateTrigger,
    TaskStateTrigger,
)

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class ExternalTaskSensorAsync(ExternalTaskSensor):
    """
    Waits asynchronously for a different DAG or a task in a different DAG to complete for a
    specific logical date.

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :param external_task_id: The task_id that contains the task you want to
        wait for. If ``None`` (default value) the sensor waits for the DAG
    :param external_task_ids: The list of task_ids that you want to wait for.
        If ``None`` (default value) the sensor waits for the DAG. Either
        external_task_id or external_task_ids can be passed to
        ExternalTaskSensor, but not both.
    :param allowed_states: Iterable of allowed states, default is ``['success']``
    :param failed_states: Iterable of failed or dis-allowed states, default is ``None``
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same logical date as the current task or DAG.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :param execution_date_fn: function that receives the current execution's logical date as the first
        positional argument and optionally any number of keyword arguments available in the
        context dictionary, and returns the desired logical dates to query.
        Either execution_delta or execution_date_fn can be passed to ExternalTaskSensor,
        but not both.
    :param check_existence: Set to `True` to check if the external task exists (when
        external_task_id is not None) or check if the DAG to wait for exists (when
        external_task_id is None), and immediately cease waiting if the external task
        or DAG does not exist (default value: False).
    """

    def execute(self, context: Context) -> None:
        """Correctly identify which trigger to execute, and defer execution as expected."""
        execution_dates = self.get_execution_dates(context)

        # Work out if we are a DAG sensor or a Task sensor
        # Defer to our trigger
        if not self.external_task_id:  # Tempting to explicitly check for None, but this captures falsy values
            self.defer(
                timeout=self.execution_timeout,
                trigger=DagStateTrigger(
                    dag_id=self.external_dag_id,
                    # The trigger does not do pass/fail, only "a state was reached",
                    # so we pass it all states that might make us pass or fail, and
                    # then work out which result we have in execute_complete.
                    states=self.allowed_states + self.failed_states,
                    execution_dates=execution_dates,
                ),
                method_name="execute_complete",
            )
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=TaskStateTrigger(
                    dag_id=self.external_dag_id,
                    task_id=self.external_task_id,
                    states=self.allowed_states + self.failed_states,
                    execution_dates=execution_dates,
                ),
                method_name="execute_complete",
            )

    @provide_session
    def execute_complete(
        self, context: Context, session: "Session", event: Optional[Dict[str, Any]] = None
    ) -> None:
        """Verifies that there is a success status for each task via execution date."""
        execution_dates = self.get_execution_dates(context)
        count_allowed = self.get_count(execution_dates, session, self.allowed_states)
        if count_allowed != len(execution_dates):
            if self.external_task_id:
                raise AirflowException(
                    f"The external task {self.external_task_id} in DAG {self.external_dag_id} failed."
                )
            else:
                raise AirflowException(f"The external DAG {self.external_dag_id} failed.")
        return None

    def get_execution_dates(self, context: Context) -> List[datetime.datetime]:
        """Helper function to set execution dates depending on which context and/or internal fields are populated."""
        if self.execution_delta:
            execution_date = context["execution_date"] - self.execution_delta
        elif self.execution_date_fn:
            execution_date = self._handle_execution_date_fn(context=context)
        else:
            execution_date = context["execution_date"]
        execution_dates = execution_date if isinstance(execution_date, list) else [execution_date]
        return execution_dates
