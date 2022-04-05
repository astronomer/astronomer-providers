import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.emr import EmrContainerHookAsync


class EmrContainerSensorTrigger(BaseTrigger):
    """
    The EmrContainerSensorTrigger is triggered when EMR container is created, it polls for the AWS EMR EKS Virtual
    Cluster Job status. It is fired as deferred class with params to run the task in trigger worker

    :param virtual_cluster_id: Reference Emr cluster id
    :param job_id:  job_id to check the state
    :param max_retries: maximum retry for poll for the status
    :param aws_conn_id: Reference to AWS connection id
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        virtual_cluster_id: str,
        job_id: str,
        max_retries: Optional[int],
        aws_conn_id: str,
        poll_interval: int,
    ):
        super().__init__()
        self.virtual_cluster_id = virtual_cluster_id
        self.job_id = job_id
        self.max_retries = max_retries
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes EmrContainerSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.emr.EmrContainerSensorTrigger",
            {
                "virtual_cluster_id": self.virtual_cluster_id,
                "job_id": self.job_id,
                "max_retries": self.max_retries,
                "poll_interval": self.poll_interval,
                "aws_conn_id": self.aws_conn_id,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make async connection to EMR container, polls for the job state"""
        hook = EmrContainerHookAsync(aws_conn_id=self.aws_conn_id, virtual_cluster_id=self.virtual_cluster_id)
        try:
            while True:
                query_status = await hook.check_job_status(job_id=self.job_id)
                if query_status is None or query_status in ("PENDING", "SUBMITTED", "RUNNING"):
                    await asyncio.sleep(self.poll_interval)
                elif query_status in ("FAILED", "CANCELLED", "CANCEL_PENDING"):
                    msg = f"EMR Containers sensor failed {query_status}"
                    yield TriggerEvent({"status": "error", "message": msg})
                    return
                else:
                    msg = "EMR Containers sensors completed"
                    yield TriggerEvent({"status": "success", "message": msg})
                    return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
