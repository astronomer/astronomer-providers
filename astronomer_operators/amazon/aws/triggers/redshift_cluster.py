import asyncio
from typing import Any, Dict, Tuple

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.amazon.aws.hooks.redshift_cluster import RedshiftHookAsync


class RedshiftClusterTrigger(BaseTrigger):
    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        aws_conn_id: str,
        cluster_identifier: str,
        operation_type: str,
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.aws_conn_id = aws_conn_id
        self.cluster_identifier = cluster_identifier
        self.operation_type = operation_type

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes RedshiftClusterTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "aws_conn_id": self.aws_conn_id,
                "cluster_identifier": self.cluster_identifier,
                "operation_type": self.operation_type,
            },
        )

    async def run(self):
        """
        Make async connection to redshift, based on the operation type call
        the RedshiftHookAsync functions
        if operation_type is 'resume_cluster' it will call the resume_cluster function in RedshiftHookAsync
        if operation_type is 'pause_cluster it will call the pause_cluster function in RedshiftHookAsync
        """
        hook = RedshiftHookAsync(aws_conn_id=self.aws_conn_id)
        while True:
            try:
                if self.operation_type == "resume_cluster":
                    response = await hook.resume_cluster(ClusterIdentifier=self.cluster_identifier)
                    if response:
                        yield TriggerEvent(response)
                        return
                    else:
                        error_message = f"{self.task_id} failed"
                        raise AirflowException(error_message)
                else:
                    response = await hook.pause_cluster(ClusterIdentifier=self.cluster_identifier)
                    if response:
                        yield TriggerEvent(response)
                        return
                    else:
                        error_message = f"{self.task_id} failed"
                        raise AirflowException(error_message)
            except AirflowException:
                await asyncio.sleep(self.polling_period_seconds)
