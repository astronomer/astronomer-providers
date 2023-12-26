import asyncio
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.emr import (
    EmrContainerHookAsync,
    EmrJobFlowHookAsync,
)


class EmrContainerBaseTrigger(BaseTrigger):
    """
    Poll for the status of EMR container until reaches terminal state

    :param virtual_cluster_id: Reference Emr cluster id
    :param job_id:  job_id to check the state
    :param max_tries: maximum try attempts for polling the status
    :param aws_conn_id: Reference to AWS connection id
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        virtual_cluster_id: str,
        job_id: str,
        aws_conn_id: str = "aws_default",
        poll_interval: int = 10,
        max_tries: Optional[int] = None,
        **kwargs: Any,
    ):
        self.virtual_cluster_id = virtual_cluster_id
        self.job_id = job_id
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.max_tries = max_tries
        super().__init__(**kwargs)


class EmrContainerOperatorTrigger(EmrContainerBaseTrigger):
    """Poll for the status of EMR container until reaches terminal state"""

    INTERMEDIATE_STATES: List[str] = ["PENDING", "SUBMITTED", "RUNNING"]
    FAILURE_STATES: List[str] = ["FAILED", "CANCELLED", "CANCEL_PENDING"]
    SUCCESS_STATES: List[str] = ["COMPLETED"]
    TERMINAL_STATES: List[str] = ["COMPLETED", "FAILED", "CANCELLED", "CANCEL_PENDING"]

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes EmrContainerOperatorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.emr.EmrContainerOperatorTrigger",
            {
                "virtual_cluster_id": self.virtual_cluster_id,
                "job_id": self.job_id,
                "aws_conn_id": self.aws_conn_id,
                "max_tries": self.max_tries,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """Run until EMR container reaches the desire state"""
        hook = EmrContainerHookAsync(aws_conn_id=self.aws_conn_id, virtual_cluster_id=self.virtual_cluster_id)
        try:
            try_number: int = 1
            while True:
                query_state = await hook.check_job_status(self.job_id)
                if query_state is None:
                    self.log.info("Try %s: Invalid query state. Retrying again", try_number)
                    await asyncio.sleep(self.poll_interval)
                elif query_state in self.FAILURE_STATES:
                    self.log.info(
                        "Try %s: Query execution completed. Final state is %s", try_number, query_state
                    )
                    error_message = await hook.get_job_failure_reason(self.job_id)
                    message = (
                        f"EMR Containers job failed. Final state is {query_state}. "
                        f"query_execution_id is {self.job_id}. Error: {error_message}"
                    )
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": message,
                            "job_id": self.job_id,
                        }
                    )
                elif query_state in self.SUCCESS_STATES:
                    self.log.info(
                        "Try %s: Query execution completed. Final state is %s", try_number, query_state
                    )
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": f"EMR Containers Operator success {query_state}",
                            "job_id": self.job_id,
                        }
                    )
                else:
                    self.log.info(
                        "Try %s: Query is still in non-terminal state - %s", try_number, query_state
                    )
                    await asyncio.sleep(self.poll_interval)
                if self.max_tries and try_number >= self.max_tries:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": "Timeout: Maximum retry limit exceed",
                            "job_id": self.job_id,
                        }
                    )

                try_number += 1
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


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

    async def run(self) -> AsyncIterator["TriggerEvent"]:
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
                elif cluster_state in self.failed_states:
                    final_message = "EMR job failed"
                    failure_message = hook.failure_message_from_response(cluster_details)
                    if failure_message:
                        final_message += " " + failure_message
                    yield TriggerEvent({"status": "error", "message": final_message})
                await asyncio.sleep(self.poll_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
