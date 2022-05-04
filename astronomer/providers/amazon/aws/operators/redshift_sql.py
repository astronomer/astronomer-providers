from typing import TYPE_CHECKING, Any, Dict, cast

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

from astronomer.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from astronomer.providers.amazon.aws.triggers.redshift_sql import RedshiftSQLTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftSQLOperatorAsync(RedshiftSQLOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster"

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param redshift_conn_id: reference to Amazon Redshift connection id
    :param parameters: (optional) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: "Context") -> None:
        """
        Makes a sync call to RedshiftDataHook and execute the query and gets back the query_ids list and
        defers trigger to poll for the status for the query executed
        """
        redshift_data_hook = RedshiftDataHook(aws_conn_id=self.redshift_conn_id)
        query_ids, response = redshift_data_hook.execute_query(sql=cast(str, self.sql), params=self.params)
        if response.get("status") == "error":
            self.execute_complete({}, response)
            return
        self.defer(
            timeout=self.execution_timeout,
            trigger=RedshiftSQLTrigger(
                task_id=self.task_id,
                polling_period_seconds=self.poll_interval,
                aws_conn_id=self.redshift_conn_id,
                query_ids=query_ids,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}".format(event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                return None
        else:
            self.log.info("%s completed successfully.", self.task_id)
            return None
