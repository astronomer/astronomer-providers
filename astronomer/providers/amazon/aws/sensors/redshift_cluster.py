import warnings
from datetime import timedelta
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor

from astronomer.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterSensorTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class RedshiftClusterSensorAsync(RedshiftClusterSensor):
    """
    Waits for a Redshift cluster to reach a specific status.

    :param cluster_identifier: The identifier for the cluster being pinged.\
    :param target_status: The cluster status desired.
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5,
        **kwargs: Any,
    ):
        # TODO: Remove once deprecated
        if poll_interval:
            self.poke_interval = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Check for the target_status and defers using the trigger"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=RedshiftClusterSensorTrigger(
                task_id=self.task_id,
                aws_conn_id=self.aws_conn_id,
                cluster_identifier=self.cluster_identifier,
                target_status=self.target_status,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Optional[Dict[Any, Any]] = None) -> None:
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
