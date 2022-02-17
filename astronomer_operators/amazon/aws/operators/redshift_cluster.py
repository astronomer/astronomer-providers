from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook

from astronomer_operators.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterTrigger,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftResumeClusterOperatorAsync(BaseOperator):
    """
    Resume a paused AWS Redshift Cluster

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    template_fields: Sequence[str] = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        aws_conn_id: str = "aws_default",
        poll_interval: float = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.poll_interval = poll_interval
        self.aws_conn_id = aws_conn_id

    def execute(self, context: "Context"):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == "paused":
            self.defer(
                timeout=self.execution_timeout,
                trigger=RedshiftClusterTrigger(
                    task_id=self.task_id,
                    polling_period_seconds=self.poll_interval,
                    aws_conn_id=self.aws_conn_id,
                    cluster_identifier=self.cluster_identifier,
                    operation_type="resume_cluster",
                ),
                method_name="execute_complete",
            )
        else:
            self.log.warning(
                "Unable to resume cluster since cluster is currently in status: %s", cluster_state
            )

    def execute_complete(self, context, event=None):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}: {1}".format(event["status"], event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info("Resumed cluster successfully, now its in available state")
                return None
        else:
            self.log.info("%s completed successfully.", self.task_id)
            return None


class RedshiftPauseClusterOperatorAsync(BaseOperator):
    """
    Pause an AWS Redshift Cluster if cluster status is in `available` state

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    template_fields: Sequence[str] = ("cluster_identifier",)
    ui_color = "#eeaa11"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        cluster_identifier: str,
        aws_conn_id: str = "aws_default",
        poll_interval: float = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval

    def execute(self, context: "Context"):
        redshift_hook = RedshiftHook(aws_conn_id=self.aws_conn_id)
        cluster_state = redshift_hook.cluster_status(cluster_identifier=self.cluster_identifier)
        if cluster_state == "available":
            self.defer(
                timeout=self.execution_timeout,
                trigger=RedshiftClusterTrigger(
                    task_id=self.task_id,
                    polling_period_seconds=self.poll_interval,
                    aws_conn_id=self.aws_conn_id,
                    cluster_identifier=self.cluster_identifier,
                    operation_type="pause_cluster",
                ),
                method_name="execute_complete",
            )
        else:
            self.log.warning(
                "Unable to pause cluster since cluster is currently in status: %s", cluster_state
            )

    def execute_complete(self, context, event=None):
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}: {1}".format(event["status"], event["message"])
                raise AirflowException(msg)
            elif "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info("Paused cluster successfully")
                return None
        else:
            self.log.info("%s completed successfully.", self.task_id)
            return None
