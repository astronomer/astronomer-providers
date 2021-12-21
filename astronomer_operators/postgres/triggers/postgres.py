import asyncio
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.postgres.hooks.postgres import PostgresHookAsync


class PostgresTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        sql: Union[str, List[str]],
        postgres_conn_id: str = "postgres_default",
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None,
        poll_interval: float = 1.0,
    ):
        self.log.info("Executing PostgresTrigger")
        super().__init__()
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.task_id = task_id
        self.parameters = parameters
        self.database = database
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes PostgresTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.postgres.triggers.postgres.PostgresTrigger",
            {
                "sql": self.sql,
                "postgres_conn_id": self.postgres_conn_id,
                "task_id": self.task_id,
                "parameters": self.parameters,
                "database": self.database,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Makes a series of asynchronous query requests via PostgresHookAsync. It yields a Trigger if
        query is successfully executed, will retry the query up to the retry limit
        if there is an error, otherwise it throws an exception.
        """
        hook = self._get_async_hook()
        while True:
            try:
                response_from_hook = await hook.run(sql=self.sql, parameters=self.parameters)
                yield TriggerEvent(response_from_hook)
                return

            except AirflowException:
                await asyncio.sleep(self.poll_interval)

    def _get_async_hook(self) -> PostgresHookAsync:
        return PostgresHookAsync(postgres_conn_id=self.postgres_conn_id, schema=self.database)
