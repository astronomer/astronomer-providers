import asyncio
import datetime
from typing import Any, Dict, List, Tuple

from airflow.exceptions import AirflowException
from airflow.models import DagRun, TaskInstance
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import provide_session
from asgiref.sync import sync_to_async
from sqlalchemy import func


class ExternalTaskSensorAsync(ExternalTaskSensor):
    def execute(self, context):
        """
        Logic that the sensor uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
        execution_dates = self.get_execution_dates(context)

        # Work out if we are a DAG sensor or a Task sensor
        # Defer to our trigger
        if (
            not self.external_task_id
        ):  # Tempting to explicitly check for None, but this captures falsy values
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
        self, context, session, event=None
    ):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Verifies that there is a success status for each task via execution date.
        """
        execution_dates = self.get_execution_dates(context)
        count_allowed = self.get_count(execution_dates, session, self.allowed_states)
        if count_allowed != len(execution_dates):
            if self.external_task_id:
                raise AirflowException(
                    f"The external task {self.external_task_id} in DAG {self.external_dag_id} failed."
                )
            else:
                raise AirflowException(
                    f"The external DAG {self.external_dag_id} failed."
                )
        return None

    def get_execution_dates(self, context):
        """
        Helper function to set execution dates depending on which context and/or
        internal fields are populated.
        """
        if self.execution_delta:
            execution_date = context["execution_date"] - self.execution_delta
        elif self.execution_date_fn:
            execution_date = self._handle_execution_date_fn(context=context)
        else:
            execution_date = context["execution_date"]
        execution_dates = (
            execution_date if isinstance(execution_date, list) else [execution_date]
        )
        return execution_dates


class TaskStateTrigger(BaseTrigger):
    def __init__(
        self,
        dag_id: str,
        task_id: str,
        states: List[str],
        execution_dates: List[datetime.datetime],
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.task_id = task_id
        self.states = states
        self.execution_dates = execution_dates
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes TaskStatetrigger arguments and classpath.
        """
        return (
            "astronomer_operators.external_task.TaskStateTrigger",
            {
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Checks periodically in the database to see if the task exists, and has
        hit one of the states yet, or not.
        """
        while True:
            num_tasks = await self.count_tasks()
            if num_tasks == len(self.execution_dates):
                yield TriggerEvent(True)
                return
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    @provide_session
    def count_tasks(self, session) -> int:
        """
        Count how many task instances in the database match our criteria.
        """
        count = (
            session.query(func.count())  # .count() is inefficient
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.state.in_(self.states),
                TaskInstance.execution_date.in_(self.execution_dates),
            )
            .scalar()
        )
        return count


class DagStateTrigger(BaseTrigger):
    def __init__(
        self,
        dag_id: str,
        states: List[str],
        execution_dates: List[datetime.datetime],
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.dag_id = dag_id
        self.states = states
        self.execution_dates = execution_dates
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes DagStateTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.external_task.DagStateTrigger",
            {
                "dag_id": self.dag_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Checks periodically in the database to see if the dag run exists, and has
        hit one of the states yet, or not.
        """

        while True:
            num_dags = await self.count_dags()
            if num_dags == len(self.execution_dates):
                yield TriggerEvent(True)
                return
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    @provide_session
    def count_dags(self, session) -> int:
        """
        Count how many dag runs in the database match our criteria.
        """
        count = (
            session.query(func.count())  # .count() is inefficient
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.state.in_(self.states),
                DagRun.execution_date.in_(self.execution_dates),
            )
            .scalar()
        )
        return count
