import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.redshift_cluster import RedshiftHookAsync


class RedshiftClusterTrigger(BaseTrigger):
    """
    RedshiftClusterTrigger is fired as deferred class with params to run the task in trigger worker

    :param task_id: Reference to task id of the Dag
    :param polling_period_seconds:  polling period in seconds to check for the status
    :param aws_conn_id: Reference to AWS connection id for redshift
    :param cluster_identifier: unique identifier of a cluster
    :param operation_type: Reference to the type of operation need to be performed
        eg: pause_cluster, resume_cluster, delete_cluster
    :param skip_final_cluster_snapshot: determines cluster snapshot creation
    :param final_cluster_snapshot_identifier: name of final cluster snapshot
    """

    def __init__(
        self,
        task_id: str,
        aws_conn_id: str,
        cluster_identifier: str,
        operation_type: str,
        polling_period_seconds: float = 5.0,
        skip_final_cluster_snapshot: bool = True,
        final_cluster_snapshot_identifier: Optional[str] = None,
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.aws_conn_id = aws_conn_id
        self.cluster_identifier = cluster_identifier
        self.operation_type = operation_type
        self.skip_final_cluster_snapshot = skip_final_cluster_snapshot
        self.final_cluster_snapshot_identifier = final_cluster_snapshot_identifier

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes RedshiftClusterTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "aws_conn_id": self.aws_conn_id,
                "cluster_identifier": self.cluster_identifier,
                "operation_type": self.operation_type,
                "skip_final_cluster_snapshot": self.skip_final_cluster_snapshot,
                "final_cluster_snapshot_identifier": self.final_cluster_snapshot_identifier,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Make async connection to redshift, based on the operation type call
        the RedshiftHookAsync functions
        if operation_type is 'resume_cluster' it will call the resume_cluster function in RedshiftHookAsync
        if operation_type is 'pause_cluster it will call the pause_cluster function in RedshiftHookAsync
        """
        hook = RedshiftHookAsync(aws_conn_id=self.aws_conn_id)
        try:
            if self.operation_type == "delete_cluster":
                response = await hook.delete_cluster(
                    cluster_identifier=self.cluster_identifier,
                    skip_final_cluster_snapshot=self.skip_final_cluster_snapshot,
                    final_cluster_snapshot_identifier=self.final_cluster_snapshot_identifier,
                    polling_period_seconds=self.polling_period_seconds,
                )
                if response:
                    yield TriggerEvent(response)
                else:
                    error_message = f"{self.task_id} failed"
                    yield TriggerEvent({"status": "error", "message": error_message})
            elif self.operation_type == "resume_cluster":
                response = await hook.resume_cluster(
                    cluster_identifier=self.cluster_identifier,
                    polling_period_seconds=self.polling_period_seconds,
                )
                if response:
                    yield TriggerEvent(response)
                else:
                    error_message = f"{self.task_id} failed"
                    yield TriggerEvent({"status": "error", "message": error_message})
            elif self.operation_type == "pause_cluster":
                response = await hook.pause_cluster(
                    cluster_identifier=self.cluster_identifier,
                    polling_period_seconds=self.polling_period_seconds,
                )
                if response:
                    yield TriggerEvent(response)
                else:
                    error_message = f"{self.task_id} failed"
                    yield TriggerEvent({"status": "error", "message": error_message})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class RedshiftClusterSensorTrigger(BaseTrigger):
    """
    RedshiftClusterSensorTrigger is fired as deferred class with params to run the task in trigger worker

    :param task_id: Reference to task id of the Dag
    :param aws_conn_id: Reference to AWS connection id for redshift
    :param cluster_identifier: unique identifier of a cluster
    :param target_status: Reference to the status which needs to be checked
    :param poke_interval:  polling period in seconds to check for the status
    """

    def __init__(
        self,
        task_id: str,
        aws_conn_id: str,
        cluster_identifier: str,
        target_status: str,
        poke_interval: float,
    ):
        super().__init__()
        self.task_id = task_id
        self.aws_conn_id = aws_conn_id
        self.cluster_identifier = cluster_identifier
        self.target_status = target_status
        self.poke_interval = poke_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes RedshiftClusterSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterSensorTrigger",
            {
                "task_id": self.task_id,
                "aws_conn_id": self.aws_conn_id,
                "cluster_identifier": self.cluster_identifier,
                "target_status": self.target_status,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Simple async function run until the cluster status match the target status."""
        try:
            hook = RedshiftHookAsync(aws_conn_id=self.aws_conn_id)
            while True:
                res = await hook.cluster_status(self.cluster_identifier)
                if (res["status"] == "success" and res["cluster_state"] == self.target_status) or res[
                    "status"
                ] == "error":
                    yield TriggerEvent(res)
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
