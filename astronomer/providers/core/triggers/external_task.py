import asyncio
import datetime
import typing
from typing import Any, AsyncIterator, Dict, List, Tuple

from airflow import AirflowException
from airflow.models import DagRun, TaskInstance
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.session import provide_session
from asgiref.sync import sync_to_async
from sqlalchemy import func
from sqlalchemy.orm import Session

from astronomer.providers.http.triggers.http import HttpTrigger


class TaskStateTrigger(BaseTrigger):
    """
    Waits asynchronously for a task in a different DAG to complete for a
    specific logical date.

    :param dag_id: The dag_id that contains the task you want to wait for
    :param task_id: The task_id that contains the task you want to
        wait for. If ``None`` (default value) the sensor waits for the DAG
    :param states: allowed states, default is ``['success']``
    :param execution_dates:
    :param poll_interval: The time interval in seconds to check the state.
        The default value is 5 sec.
    """

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
        """Serializes TaskStateTrigger arguments and classpath."""
        return (
            "astronomer.providers.core.triggers.external_task.TaskStateTrigger",
            {
                "dag_id": self.dag_id,
                "task_id": self.task_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> typing.AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Checks periodically in the database to see if the task exists, and has
        hit one of the states yet, or not.
        """
        while True:
            num_tasks = await self.count_tasks()
            if num_tasks == len(self.execution_dates):
                yield TriggerEvent(True)
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    @provide_session
    def count_tasks(self, session: Session) -> typing.Optional[int]:
        """Count how many task instances in the database match our criteria."""
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
        return typing.cast(int, count)


class DagStateTrigger(BaseTrigger):
    """
    Waits asynchronously for a task in a different DAG to complete for a
    specific logical date.

    :param dag_id: The dag_id that contains the task you want to wait for
    :param task_id: The task_id that contains the task you want to
        wait for. If ``None`` (default value) the sensor waits for the DAG
    :param states: allowed states, default is ``['success']``
    :param execution_dates: The logical date at which DAG run.
    :param poll_interval: The time interval in seconds to check the state.
        The default value is 5.0 sec.
    """

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
        """Serializes DagStateTrigger arguments and classpath."""
        return (
            "astronomer.providers.core.triggers.external_task.DagStateTrigger",
            {
                "dag_id": self.dag_id,
                "states": self.states,
                "execution_dates": self.execution_dates,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> typing.AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Checks periodically in the database to see if the dag run exists, and has
        hit one of the states yet, or not.
        """
        while True:
            num_dags = await self.count_dags()
            if num_dags == len(self.execution_dates):
                yield TriggerEvent(True)
            await asyncio.sleep(self.poll_interval)

    @sync_to_async
    @provide_session
    def count_dags(self, session: Session) -> typing.Optional[int]:
        """Count how many dag runs in the database match our criteria."""
        count = (
            session.query(func.count())  # .count() is inefficient
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.state.in_(self.states),
                DagRun.execution_date.in_(self.execution_dates),
            )
            .scalar()
        )
        return typing.cast(int, count)


class ExternalDeploymentTaskTrigger(HttpTrigger):
    """ExternalDeploymentTaskTrigger Inherits from HttpTrigger and make Async http call to get the deployment state"""

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes ExternalDeploymentTaskTrigger arguments and classpath."""
        return (
            "astronomer.providers.core.triggers.external_task.ExternalDeploymentTaskTrigger",
            {
                "endpoint": self.endpoint,
                "data": self.data,
                "headers": self.headers,
                "extra_options": self.extra_options,
                "http_conn_id": self.http_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes a series of asynchronous http calls via an http hook poll for state of the job
        run until it reaches a failure state or success state. It yields a Trigger if response state is successful.
        """
        from airflow.utils.state import State

        hook = self._get_async_hook()
        while True:
            try:
                response = await hook.run(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    extra_options=self.extra_options,
                )
                resp_json = await response.json()
                if resp_json["state"] in State.finished:
                    yield TriggerEvent(resp_json)
                else:
                    await asyncio.sleep(self.poke_interval)
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poke_interval)
                yield TriggerEvent({"state": "error", "message": str(exc)})
