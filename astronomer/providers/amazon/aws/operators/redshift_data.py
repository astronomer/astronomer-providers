from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

from astronomer.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from astronomer.providers.amazon.aws.triggers.redshift_data import RedshiftDataTrigger
from astronomer.providers.utils.typing_compat import Context


class RedshiftDataOperatorAsync(RedshiftDataOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster.
    If there are multiple queries as part of the SQL, and one of them fails to reach a successful completion state,
    the operator returns the relevant error for the failed query.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param aws_conn_id: AWS connection ID
    :param parameters: (optional) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    """

    def __init__(
        self,
        *,
        poll_interval: int = 5,
        **kwargs: Any,
    ) -> None:
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:  # type: ignore[override]
        """
        Makes a sync call to RedshiftDataHook, executes the query and gets back the list of query_ids and
        defers trigger to poll for the status for the queries executed.
        """
        redshift_data_hook = RedshiftDataHook(aws_conn_id=self.aws_conn_id)
        query_ids, response = redshift_data_hook.execute_query(sql=self.sql, params=self.params)
        self.log.info("Query IDs %s", query_ids)
        if response.get("status") == "error":
            self.execute_complete(context, event=response)
        context["ti"].xcom_push(key="return_value", value=query_ids)

        still_running = False
        completed_query_ids = []
        for qid in query_ids:
            resp = redshift_data_hook.conn.describe_statement(Id=qid)
            status = resp["Status"]
            if status == "FAILED":
                err_msg = f"Error: {resp['QueryString']} query Failed due to {resp['Error']}"
                msg = f"context: {context}, error message: {err_msg}"
                raise AirflowException(msg)
            elif status == "ABORTED":
                err_msg = "The query run was stopped by the user."
                msg = f"context: {context}, error message: {err_msg}"
                raise AirflowException(msg)
            elif status in ("SUBMITTED", "PICKED", "STARTED"):
                still_running = True
                break
            elif status == "FINISHED":
                completed_query_ids.append(qid)

        if not still_running and len(completed_query_ids) == len(query_ids):
            self.log.info("%s completed successfully.", self.task_id)
            return

        self.defer(
            timeout=self.execution_timeout,
            trigger=RedshiftDataTrigger(
                task_id=self.task_id,
                poll_interval=self.poll_interval,
                aws_conn_id=self.aws_conn_id,
                query_ids=query_ids,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "context: {}, error message: {}".format(context, event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
        else:
            raise AirflowException("Did not receive valid event from the trigerrer")
