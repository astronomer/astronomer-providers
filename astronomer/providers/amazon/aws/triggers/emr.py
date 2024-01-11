from __future__ import annotations

import asyncio
import warnings
from typing import Any, AsyncIterator, Iterable

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.emr import (
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
        max_tries: int | None = None,
        **kwargs: Any,
    ):
        self.virtual_cluster_id = virtual_cluster_id
        self.job_id = job_id
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.max_tries = max_tries
        super().__init__(**kwargs)


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
        target_states: Iterable[str] | None = None,
        failed_states: Iterable[str] | None = None,
    ):
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use :class: `~airflow.providers.amazon.aws.triggers.emr.EmrTerminateJobFlowTrigger."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__()
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.target_states = target_states or ["TERMINATED"]
        self.failed_states = failed_states or ["TERMINATED_WITH_ERRORS"]

    def serialize(self) -> tuple[str, dict[str, Any]]:
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

    async def run(self) -> AsyncIterator[TriggerEvent]:
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
