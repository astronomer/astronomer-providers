import logging
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator

from astronomer_operators.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterSensorTrigger,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)


class RedshiftClusterSensorAsync(BaseOperator):
    """
    Waits for a Redshift cluster to reach a specific status.

    :param cluster_identifier: The identifier for the cluster being pinged.\
    :param target_status: The cluster status desired.
    """

    template_fields: Sequence[str] = ("cluster_identifier", "target_status")

    def __init__(
        self,
        *,
        cluster_identifier: str,
        target_status: str = "available",
        aws_conn_id: str = "aws_default",
        poll_interval: float = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cluster_identifier = cluster_identifier
        self.target_status = target_status
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval

    def execute(self, context: "Context"):
        self.defer(
            timeout=self.execution_timeout,
            trigger=RedshiftClusterSensorTrigger(
                task_id=self.task_id,
                aws_conn_id=self.aws_conn_id,
                cluster_identifier=self.cluster_identifier,
                target_status=self.target_status,
                polling_period_seconds=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}: {1}".format(event["status"], event["message"])
                raise AirflowException(msg)
            if "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info(
                    "Cluster Identifier %s is in %s state", self.cluster_identifier, self.target_status
                )
                return None
        self.log.info("%s completed successfully.", self.task_id)
        return None
