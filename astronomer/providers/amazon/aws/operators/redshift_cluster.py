from typing import TYPE_CHECKING, Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)

from astronomer.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterTrigger,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class RedshiftResumeClusterOperatorAsync(RedshiftResumeClusterOperator):
    """
    Resume a paused AWS Redshift Cluster

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5,
        **kwargs: Any,
    ):
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: "Context") -> None:
        """
        Logic that the operator uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
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

    def execute_complete(self, context: Dict[str, Any], event: Any = None) -> None:
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


class RedshiftPauseClusterOperatorAsync(RedshiftPauseClusterOperator):
    """
    Pause an AWS Redshift Cluster if cluster status is in `available` state

    :param cluster_identifier: id of the AWS Redshift Cluster
    :param aws_conn_id: aws connection to use
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5,
        **kwargs: Any,
    ):
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: "Context") -> None:
        """
        Logic that the operator uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
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

    def execute_complete(self, context: Dict[str, Any], event: Any = None) -> None:
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
