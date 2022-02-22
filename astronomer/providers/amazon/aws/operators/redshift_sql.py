from typing import TYPE_CHECKING

from airflow import AirflowException
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

from astronomer.providers.amazon.aws.triggers.redshift_sql import RedshiftSQLTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftSQLOperatorAsync(RedshiftSQLOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster
    """

    def __init__(
        self,
        *,
        cluster_identifier: str,
        poll_interval: float = 5,
        **kwargs,
    ) -> None:
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: "Context"):
        self.defer(
            timeout=self.execution_timeout,
            trigger=RedshiftSQLTrigger(
                task_id=self.task_id,
                polling_period_seconds=self.poll_interval,
                redshift_conn_id=self.redshift_conn_id,
                cluster_identifier=self.cluster_identifier,
                sql=self.sql,
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
                self.log.info("%s completed successfully.", self.task_id)
                return None
        else:
            self.log.info("%s completed successfully.", self.task_id)
            return None
