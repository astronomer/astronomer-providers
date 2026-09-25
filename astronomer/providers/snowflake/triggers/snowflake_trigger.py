from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async

from astronomer.providers.snowflake.hooks.snowflake import (
    SnowflakeHookAsync,
    fetch_all_snowflake_handler,
)


def get_db_hook(snowflake_conn_id: str) -> SnowflakeHookAsync:
    """
    Create and return SnowflakeHookAsync.
    :return: a SnowflakeHookAsync instance.
    """
    return SnowflakeHookAsync(snowflake_conn_id=snowflake_conn_id)


class SnowflakeSensorTrigger(BaseTrigger):
    """
    This trigger validates the result of a query (asynchronously).
    An Airflow Trigger asynchronously polls for a certain condition to be true (which yields a
    ``TriggerEvent``), after which a synchronous piece of code can be used to complete the logic (set by
    ``method_name`` on AsyncOperator/Sensor.defer()).
    Docs: https://airflow.apache.org/docs/apache-airflow/stable/concepts/deferring.html#triggering-deferral
    """

    def __init__(
        self,
        sql: str,
        dag_id: str,
        task_id: str,
        run_id: str,
        snowflake_conn_id: str,
        parameters: str | None = None,
        success: str | None = None,
        failure: str | None = None,
        fail_on_empty: bool = False,
        poke_interval: float = 60,
    ):
        super().__init__()
        self._sql = sql
        self._parameters = parameters
        self._success = success
        self._failure = failure
        self._fail_on_empty = fail_on_empty
        self._dag_id = dag_id
        self._task_id = task_id
        self._run_id = run_id
        self._conn_id = snowflake_conn_id
        self._poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes SqlTrigger arguments and classpath.."""
        return (
            "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeSensorTrigger",
            {
                "sql": self._sql,
                "parameters": self._parameters,
                "poke_interval": self._poke_interval,
                "success": self._success,
                "failure": self._failure,
                "fail_on_empty": self._fail_on_empty,
                "dag_id": self._dag_id,
                "task_id": self._task_id,
                "run_id": self._run_id,
                "snowflake_conn_id": self._conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Make an asynchronous connection to Snowflake and defer until query
        returns a result
        """
        try:
            hook = get_db_hook(self._conn_id)
            while True:
                query_ids = await sync_to_async(hook.run)(
                    self._sql,
                    parameters=self._parameters,  # type: ignore[arg-type]
                )
                run_state = await hook.get_query_status(query_ids, 5)
                if run_state:
                    result = await sync_to_async(hook.check_query_output)(
                        query_ids=query_ids,
                        handler=fetch_all_snowflake_handler,
                    )

                    self.log.info(
                        "Raw query result = %s <DAG id = %s, task id = %s, run id = %s>",
                        result,
                        self._dag_id,
                        self._task_id,
                        self._run_id,
                    )
                    if result is not None:
                        yield TriggerEvent(
                            {"status": "validate", "result": result, "message": "waiting to validate query"}
                        )
                        return
                    else:
                        self.log.info(
                            (
                                "No success yet. Checking again in %s seconds. "
                                "<DAG id = %s, task id = %s, run id = %s>"
                            ),
                            self._poke_interval,
                            self._dag_id,
                            self._task_id,
                            self._run_id,
                        )
                        await asyncio.sleep(self._poke_interval)
                else:
                    error_message = f"{self._task_id} failed with terminal state: {run_state}"
                    yield TriggerEvent({"status": "error", "message": error_message})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
