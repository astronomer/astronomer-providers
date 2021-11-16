import asyncio
import datetime
from typing import Any, Dict, List, Tuple

from airflow.models import DagRun
from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async
from sqlalchemy import func
from utils.session import provide_session


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
            "astronomer_operators.triggers.external_dag.DagStateTrigger",
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
