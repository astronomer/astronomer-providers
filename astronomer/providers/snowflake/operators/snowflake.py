from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeTrigger,
    get_db_hook,
)


class SnowflakeOperatorAsync(SnowflakeOperator):
    def __init__(self, *, poll_interval: int = 5, **kwargs):
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def get_db_hook(self) -> SnowflakeHookAsync:
        """Get the Snowflake Hook"""
        return get_db_hook(self)

    def execute(self, context):
        """
        Make a sync connection to snowflake and run query in execute_async
        function in snowflake and close the connection
        """
        self.log.info("Executing: %s", self.sql)
        hook = self.get_db_hook()
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)
        self.query_ids = hook.query_ids

        if self.do_xcom_push:
            context["ti"].xcom_push(key="query_ids", value=self.query_ids)

        self.defer(
            timeout=self.execution_timeout,
            trigger=SnowflakeTrigger(
                task_id=self.task_id,
                polling_period_seconds=self.poll_interval,
                query_ids=self.query_ids,
                snowflake_conn_id=self.snowflake_conn_id,
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
                msg = "{0}: {1}".format(event["type"], event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                hook = self.get_db_hook()
                hook.check_query_output(event["query_ids"])
                self.log.info("%s completed successfully.", self.task_id)
                return None
        else:
            self.log.info("%s completed successfully.", self.task_id)
            return None
