from typing import Any, cast

from airflow.exceptions import AirflowException

try:
    from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
except ImportError:  # pragma: no cover
    # For apache-airflow-providers-amazon > 6.0.0
    # currently added type: ignore[no-redef, attr-defined] and pragma: no cover because this import
    # path won't be available in current setup
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as RedshiftSQLOperator  # type: ignore[assignment] # noqa: E501  # pragma: no cover

from astronomer.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from astronomer.providers.amazon.aws.triggers.redshift_sql import RedshiftSQLTrigger
from astronomer.providers.utils.typing_compat import Context


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
        redshift_conn_id: str = "redshift_default",
        poll_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        self.redshift_conn_id = redshift_conn_id
        self.poll_interval = poll_interval
        if self.__class__.__base__.__name__ == "RedshiftSQLOperator":
            # It's better to do str check of the parent class name because currently RedshiftSQLOperator
            # is deprecated and in future OSS RedshiftSQLOperator may be removed
            super().__init__(**kwargs)
        else:
            super().__init__(conn_id=redshift_conn_id, **kwargs)  # pragma: no cover

    def execute(self, context: Context) -> None:
        """
        Makes a sync call to RedshiftDataHook and execute the query and gets back the query_ids list and
        defers trigger to poll for the status for the query executed
        """
        redshift_data_hook = RedshiftDataHook(aws_conn_id=self.redshift_conn_id)
        query_ids, response = redshift_data_hook.execute_query(sql=cast(str, self.sql), params=self.params)
        if response.get("status") == "error":
            self.execute_complete(cast(Context, {}), response)
            return
        context["ti"].xcom_push(key="return_value", value=query_ids)
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

    def execute_complete(self, context: Context, event: Any = None) -> None:
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
