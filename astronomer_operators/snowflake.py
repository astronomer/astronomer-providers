import asyncio
from subprocess import run
from typing import Any, Dict, List, Tuple, Optional

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator
)
from airflow.triggers.base import BaseTrigger, TriggerEvent
from sqlalchemy.orm import query

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

    def __init__(self, *, poll_interval: int = 5, **kwargs):
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def get_db_hook(self) -> SnowflakeHookAsync:
        """Get the Snowflake Hook """
        return get_db_hook(self)

    def execute(self, context):
        """
        Make a sync connection to snowflake and run query in execute_async 
        function in snowflake and close the connection
        """
        self.log.info('Executing: %s', self.sql)
        hook = self.get_db_hook()
        self.log.info(self.sql)
        hook.run_async(self.sql, autocommit=self.autocommit, parameters=self.parameters)
        self.query_ids = hook.query_ids
        self.log.info("self.query_ids ", self.query_ids)

        if self.do_xcom_push:
            context["ti"].xcom_push(key='query_ids', value=self.query_ids)
        
        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeTrigger(
                task_id=self.task_id,
                polling_period_seconds=self.poll_interval,
                query_ids=self.query_ids,
                snowflake_conn_id=self.snowflake_conn_id,
                warehouse=self.warehouse,
                database=self.database,
                role=self.role,
                schema=self.schema,
                authenticator=self.authenticator,
                session_parameters=self.session_parameters,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                raise AirflowException(event["type"] +': '+ event["message"])
            elif "status" in event and event["status"] == "success":
                hook = self.get_db_hook()
                hook.check_query_output(event["query_ids"])
                self.log.info("%s completed successfully.", self.task_id)
                return None
        else:
            self.log.info("%s completed successfully.", self.task_id)
            return None

class SnowflakeTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        polling_period_seconds: int,
        query_ids: List[str],
        snowflake_conn_id: str,
        warehouse: Optional[str],
        database: Optional[str],
        role: Optional[str],
        schema: Optional[str],
        authenticator: Optional[str],
        session_parameters: Optional[str],
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.query_ids = query_ids
        self.snowflake_conn_id = snowflake_conn_id
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes SnowflakeTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.snowflake.SnowflakeTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "query_ids": self.query_ids,
                "snowflake_conn_id": self.snowflake_conn_id,
                "warehouse": self.warehouse,
                "database": self.database,
                "role" :self.role,
                "schema" :self.schema,
                "authenticator": self.authenticator,
                "session_parameters": self.session_parameters
            },
        )

    async def run(self):
        """
        Makes a series of connection to snowflake to get the status of the query by async get_query_status function
        """
        hook = get_db_hook(self)
        while True:
            run_state = await hook.get_query_status(self.query_ids)
            if run_state:
                yield TriggerEvent(run_state)
                return
            else:
                self.log.info("%s in run state: %s", self.task_id, run_state)
                self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
                await asyncio.sleep(self.polling_period_seconds)
                
