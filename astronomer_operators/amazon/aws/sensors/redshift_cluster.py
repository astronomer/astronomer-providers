import logging
from typing import TYPE_CHECKING, Sequence

from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor

from astronomer_operators.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterSensorTrigger,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)


class RedshiftClusterSensorAsync(RedshiftClusterSensor):
    """
    Waits for a Redshift cluster to reach a specific status.

    :param cluster_identifier: The identifier for the cluster being pinged.
    :type cluster_identifier: str
    :param target_status: The cluster status desired.
    :type target_status: str
    """

    template_fields: Sequence[str] = ("cluster_identifier", "target_status")

    def execute(self, context: Context):
        if not self.poke(context=context):
            self.defer(
                timeout=self.execution_timeout,
                trigger=RedshiftClusterSensorTrigger(
                    task_id=self.task_id,
                    aws_conn_id=self.aws_conn_id,
                    cluster_identifier=self.cluster_identifier,
                    target_status=self.target_status,
                    polling_period_seconds=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None
