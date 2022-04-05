import asyncio
from typing import Any, AsyncIterator, Dict, Iterable, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.emr_job_flow import EmrJobFlowHookAsync


class EmrJobFlowSensorTrigger(BaseTrigger):
    """
    EmrJobFlowSensorTrigger is fired as deferred class with params to run the task in trigger worker, when
    EMR JobFlow is created

    :param job_flow_id: job_flow_id to check the state of
    :param target_states: the target states, sensor waits until
        job flow reaches any of these states
    :param failed_states: the failure states, sensor fails when
        job flow reaches any of these states
    :param poll_interval:  polling period in seconds to check for the status
    """

    def __init__(
        self,
        job_flow_id: str,
        aws_conn_id: str,
        poll_interval: float,
        target_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
    ):
        super().__init__()
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.target_states = target_states or ["TERMINATED"]
        self.failed_states = failed_states or ["TERMINATED_WITH_ERRORS"]

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes EmrJobFlowSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.emr.EmrJobFlowSensorTrigger",
            {
                "job_flow_id": self.job_flow_id,
                "aws_conn_id": self.aws_conn_id,
                "target_states": self.target_states,
                "failed_states": self.failed_states,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make async connection to EMR container, polls for the target job state"""
        hook = EmrJobFlowHookAsync(aws_conn_id=self.aws_conn_id)
        try:
            while True:
                cluster_details = await hook.get_cluster_details(self.job_flow_id)
                cluster_state = hook.state_from_response(cluster_details)
                if cluster_state in self.target_states:
                    yield TriggerEvent(
                        {"status": "success", "message": f"Job flow currently {cluster_state}"}
                    )
                    return
                elif cluster_state in self.failed_states:
                    final_message = "EMR job failed"
                    failure_message = hook.failure_message_from_response(cluster_details)
                    print("failure_message ", failure_message)
                    if failure_message:
                        final_message += " " + failure_message
                    yield TriggerEvent({"status": "error", "message": final_message})
                    return
                await asyncio.sleep(self.poll_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
