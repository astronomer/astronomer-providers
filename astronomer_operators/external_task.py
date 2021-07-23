import datetime
from typing import Any, Dict, List, Tuple

from airflow.sensors.external_task import ExternalTaskSensor
from airflow.triggers.base import BaseTrigger
from asgiref.sync import sync_to_async


class ExternalTaskSensorAsync(ExternalTaskSensor):
    def execute(self, context):
        # Work out what the execution dates we want are
        if self.execution_delta:
            execution_date = context["execution_date"] - self.execution_delta
        elif self.execution_date_fn:
            execution_date = self._handle_execution_date_fn(context=context)
        else:
            execution_date = context["execution_date"]
        execution_dates = (
            execution_date if isinstance(execution_date, list) else [execution_date]
        )
        # Work out if we are a DAG sensor or a Task sensor
        # Defer to our trigger
        self.defer(
            trigger=TaskStateTrigger(
                dag_id=self.external_dag_id,
                task_id=self.external_task_id,
                # The trigger does not do pass/fail, only "a state was reached",
                # so we pass it all states that might make us pass or fail, and
                # then work out which result we have in execute_complete.
                states=self.allowed_states + self.failed_states,
                execution_dates=execution_dates,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """Callback for when the trigger fires - returns immediately."""
        # TODO: Examine the task/dag and see if we should succeed or fail
        return None


class TaskStateTrigger(BaseTrigger):
    def __init__(
        self,
        dag_id: str,
        task_id: str,
        states: List[str],
        execution_dates: List[datetime.datetime],
    ):
        super().__init__()
        self.dag_id = dag_id
        self.task_id = task_id
        self.states = states
        self.execution_dates = execution_dates

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "astronomer_operators.external_task.ExternalTaskTrigger",
            {
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
            },
        )

    async def run(self):
        """
        Check periodically in the database to see if the task exists, and has
        hit one of the states yet, or not.
        """
        # In a loop:
        #   Query the database and see if there are matching tasks
        #   Sleep

    @sync_to_async
    def count_tasks(self) -> int:
        """
        See how many tasks in the database match our criteria.
        """
        # TODO: Run database query and return count
