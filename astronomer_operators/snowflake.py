import asyncio
from os import stat_result
from typing import Any, Dict, Tuple

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.hooks.snowflake import SnowflakeHookAsync


def get_db_hook(self) -> SnowflakeHookAsync:
    """
    Create and return SnowflakeHookAsync.
    :return: a SnowflakeHookAsync instance.
    :rtype: SnowflakeHookAsync
    """
    return SnowflakeHookAsync(
        snowflake_conn_id=self.snowflake_conn_id,
        warehouse=self.warehouse,
        database=self.database,
        role=self.role,
        schema=self.schema,
        authenticator=self.authenticator,
        session_parameters=self.session_parameters,
    )

class SnowflakeOperatorAsyn(SnowflakeOperator):

    def get_db_hook(self) -> SnowflakeHookAsync:
        return get_db_hook(self)

    def execute(self, context):
        """Run query in Async on snowflake"""
        self.log.info('Executing: %s', self.sql)
        hook = self.get_db_hook()
        hook.run_async(self.sql, autocommit=self.autocommit, parameters=self.parameters)
        self.query_ids = hook.query_ids

        if self.do_xcom_push:
            context["ti"].xcom_push(key='query_ids', value=self.query_ids)

        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeTrigger(
                task_id=self.task_id,
                run_id=self.run_id,
                polling_period_seconds=self.polling_period_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None

class SnowflakeTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        run_id: str,
        polling_period_seconds: int,
    ):
        super().__init__()
        self.task_id = task_id
        self.run_id = run_id
        self.polling_period_seconds = polling_period_seconds

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes SnowflakeTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.snowflake.SnowflakeTrigger",
            {
                "conn_id": self.conn_id,
                "task_id": self.task_id,
                "run_id": self.run_id,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self):
        """
        Makes a series of asynchronous connection to snowflake to get the status of the query 
        """
        hook = get_db_hook()
        while True:
            run_state = await hook.get_query_status(self.run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    yield TriggerEvent(True)
                    return
                else:
                    error_message = (
                        f"{self.task_id} failed with terminal state: {run_state}"
                    )
                    raise AirflowException(error_message)
            else:
                self.log.info("%s in run state: %s", self.task_id, run_state)
                self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
                await asyncio.sleep(self.polling_period_seconds)
