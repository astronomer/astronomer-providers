from __future__ import annotations

import asyncio
import warnings
from datetime import timedelta
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent
from asgiref.sync import sync_to_async

from astronomer.providers.snowflake.hooks.snowflake import (
    SnowflakeHookAsync,
    fetch_all_snowflake_handler,
)
from astronomer.providers.snowflake.hooks.snowflake_sql_api import (
    SnowflakeSqlApiHookAsync,
)


def get_db_hook(snowflake_conn_id: str) -> SnowflakeHookAsync:
    """
    Create and return SnowflakeHookAsync.
    :return: a SnowflakeHookAsync instance.
    """
    return SnowflakeHookAsync(snowflake_conn_id=snowflake_conn_id)


class SnowflakeTrigger(BaseTrigger):
    """
    Snowflake Trigger inherits from the BaseTrigger,it is fired as
    deferred class with params to run the task in trigger worker and
    fetch the status for the query ids passed

    :param task_id: Reference to task id of the Dag
    :param poll_interval:  polling period in seconds to check for the status
    :param query_ids: List of Query ids to run and poll for the status
    :param snowflake_conn_id: Reference to Snowflake connection id
    """

    def __init__(
        self,
        task_id: str,
        poll_interval: float,
        query_ids: list[str],
        snowflake_conn_id: str,
    ):
        super().__init__()
        self.task_id = task_id
        self.poll_interval = poll_interval
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes SnowflakeTrigger arguments and classpath."""
        return (
            "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeTrigger",
            {
                "task_id": self.task_id,
                "poll_interval": self.poll_interval,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Makes a series of connections to snowflake to get the status of the query
        by async get_query_status function
        """
        hook = get_db_hook(self.snowflake_conn_id)
        try:
            run_state = await hook.get_query_status(self.query_ids, self.poll_interval)
            if run_state:
                yield TriggerEvent(run_state)
            else:
                error_message = f"{self.task_id} failed with terminal state: {run_state}"
                yield TriggerEvent({"status": "error", "message": error_message})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class SnowflakeSqlApiTrigger(BaseTrigger):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.snowflake.triggers.snowflake_trigger.SnowflakeSqlApiTrigger` instead.
    """

    def __init__(
        self,
        poll_interval: float,
        query_ids: list[str],
        snowflake_conn_id: str,
        token_life_time: timedelta,
        token_renewal_delta: timedelta,
    ):
        warnings.warn(
            (
                "This class is deprecated and will be removed in 2.0.0."
                "Use `airflow.providers.snowflake.triggers.snowflake_trigger.SnowflakeSqlApiTrigger` instead"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__()
        self.poll_interval = poll_interval
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id
        self.token_life_time = token_life_time
        self.token_renewal_delta = token_renewal_delta

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes SnowflakeSqlApiTrigger arguments and classpath."""
        return (
            "astronomer.providers.snowflake.triggers.snowflake_trigger.SnowflakeSqlApiTrigger",
            {
                "poll_interval": self.poll_interval,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
                "token_life_time": self.token_life_time,
                "token_renewal_delta": self.token_renewal_delta,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Makes a GET API request to snowflake with query_id to get the status of the query
        by get_sql_api_query_status async function
        """
        hook = SnowflakeSqlApiHookAsync(
            self.snowflake_conn_id,
            self.token_life_time,
            self.token_renewal_delta,
        )
        try:
            statement_query_ids: list[str] = []
            for query_id in self.query_ids:
                while await self.is_still_running(query_id):
                    await asyncio.sleep(self.poll_interval)
                statement_status = await hook.get_sql_api_query_status(query_id)
                if statement_status["status"] == "error":
                    yield TriggerEvent(statement_status)
                if statement_status["status"] == "success":
                    statement_query_ids.extend(statement_status["statement_handles"])
            yield TriggerEvent(
                {
                    "status": "success",
                    "statement_query_ids": statement_query_ids,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})

    async def is_still_running(self, query_id: str) -> bool:
        """
        Async function to check whether the query statement submitted via SQL API is still
        running state and returns True if it is still running else
        return False
        """
        hook = SnowflakeSqlApiHookAsync(
            self.snowflake_conn_id,
            self.token_life_time,
            self.token_renewal_delta,
        )
        statement_status = await hook.get_sql_api_query_status(query_id)
        if statement_status["status"] in ["running"]:
            return True
        return False


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
                    self._sql, parameters=self._parameters  # type: ignore[arg-type]
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
