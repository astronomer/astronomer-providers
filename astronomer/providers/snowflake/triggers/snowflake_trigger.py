import asyncio
from typing import Any, Dict, List, Tuple

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync


def get_db_hook(self) -> SnowflakeHookAsync:
    """
    Create and return SnowflakeHookAsync.
    :return: a SnowflakeHookAsync instance.
    :rtype: SnowflakeHookAsync
    """
    return SnowflakeHookAsync(
        snowflake_conn_id=self.snowflake_conn_id,
    )


class SnowflakeTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        query_ids: List[str],
        snowflake_conn_id: str,
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes SnowflakeTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
            },
        )

    async def run(self):
        """
        Makes a series of connection to snowflake to get the status of the query by async get_query_status function
        """
        hook = get_db_hook(self)
        while True:
            try:
                run_state = await hook.get_query_status(self.query_ids)
                if run_state:
                    yield TriggerEvent(run_state)
                    return
                else:
                    error_message = f"{self.task_id} failed with terminal state: {run_state}"
                    raise AirflowException(error_message)
            except AirflowException:
                await asyncio.sleep(self.polling_period_seconds)
