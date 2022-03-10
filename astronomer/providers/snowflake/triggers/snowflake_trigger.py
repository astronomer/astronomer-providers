from typing import Any, AsyncIterator, Dict, List, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync


def get_db_hook(snowflake_conn_id: str) -> SnowflakeHookAsync:
    """
    Create and return SnowflakeHookAsync.
    :return: a SnowflakeHookAsync instance.
    """
    return SnowflakeHookAsync(snowflake_conn_id=snowflake_conn_id)


class SnowflakeTrigger(BaseTrigger):  # noqa: D101
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
        """Serializes SnowflakeTrigger arguments and classpath."""
        return (
            "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes a series of connections to snowflake to get the status of the query
        by async get_query_status function
        """
        hook = get_db_hook(self.snowflake_conn_id)
        try:
            run_state = await hook.get_query_status(self.query_ids)
            if run_state:
                yield TriggerEvent(run_state)
                return
            else:
                error_message = f"{self.task_id} failed with terminal state: {run_state}"
                yield TriggerEvent({"status": "error", "message": str(error_message), "type": "ERROR"})
                return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "type": "ERROR"})
            return
