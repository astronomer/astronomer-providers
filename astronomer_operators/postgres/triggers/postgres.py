import asyncio
from typing import Any, Dict, List, Optional, Tuple, Union

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.postgres.hooks.postgres import PostgresHookAsync


class PostgresTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        sql: Union[str, List[str]],
        postgres_conn_id: str = "postgres_default",
        application_name: Optional[str] = None,
        pid: Optional[int] = None,
        poll_interval: float = 2.0,
    ):
        super().__init__()
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.task_id = task_id
        self.application_name = application_name
        self.pid = pid
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
                "application_name": self.application_name,
                "pid": self.pid,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Makes a series of asynchronous query requests via PostgresHookAsync. It yields a Trigger if
        query is successfully executed, will retry the query up to the retry limit
        if there is an error, otherwise it throws an exception.
        """
        hook = PostgresHookAsync(postgres_conn_id=self.postgres_conn_id)
        sql_to_check = f"""
        SELECT state, age(clock_timestamp(), query_start) FROM pg_stat_activity
        WHERE application_name = '{self.application_name}' and pid = '{self.pid}'
        """
        self.log.debug("Checking to see if the SQL query has already been executed")
        self.log.debug("Running SQL: %s", sql_to_check)
        execution_age = None
        completion_message = "Query Execution completed. "

        while True:
            try:
                response_from_hook = await hook.get_first(sql_to_check)
                self.log.debug("Response from hook: %s", response_from_hook)

                # When the query is completed and the records is no longer in "pg_stat_activity"
                if not response_from_hook:
                    if execution_age:
                        completion_message += f"Time taken: {execution_age}"
                    yield TriggerEvent({"status": "success", "message": completion_message})
                    return

                state = response_from_hook.get("state", None)
                execution_age = response_from_hook.get("age", execution_age)

                # When the query is completed it is in "idle" state
                if state in ["idle", ""]:
                    if execution_age:
                        completion_message += f"Time taken: {execution_age}"
                    yield TriggerEvent({"status": "success", "message": completion_message})
                    return

                if state in ["idle in transaction (aborted)"]:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": "One of the statements in the transaction caused an error.",
                        }
                    )
                    return

                # When the query is running it is in "active" state
                self.log.info(
                    "Query is still running. Time elapsed: %s. State: %s",
                    execution_age,
                    state,
                )
                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return
