import asyncio
from typing import Any, AsyncIterator, Dict, Iterable, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.emr import (
    EmrContainerHookAsync,
    EmrJobFlowHookAsync,
    EmrStepSensorHookAsync,
)


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


class EmrStepSensorTrigger(BaseTrigger):
    """
    A trigger that fires once AWS EMR cluster step reaches either target or failed state

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id: step to check the state of
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :param poke_interval: Time in seconds to wait between two consecutive call to
        check emr cluster step state
    :param target_states: the target states, sensor waits until
        step reaches any of these states
    :param failed_states: the failure states, sensor fails when
        step reaches any of these states
    """

    def __init__(
        self,
        job_flow_id: str,
        step_id: str,
        aws_conn_id: str,
        poke_interval: float,
        target_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
    ):
        super().__init__()
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.aws_conn_id = aws_conn_id
        self.poke_interval = poke_interval
        self.target_states = target_states or ["COMPLETED"]
        self.failed_states = failed_states or ["CANCELLED", "FAILED", "INTERRUPTED"]

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes EmrStepSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.emr.EmrStepSensorTrigger",
            {
                "job_flow_id": self.job_flow_id,
                "step_id": self.step_id,
                "aws_conn_id": self.aws_conn_id,
                "poke_interval": self.poke_interval,
                "target_states": self.target_states,
                "failed_states": self.failed_states,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Run until AWS EMR cluster step reach target or failed state"""
        hook = EmrStepSensorHookAsync(
            aws_conn_id=self.aws_conn_id, job_flow_id=self.job_flow_id, step_id=self.step_id
        )
        try:
            while True:
                response = await hook.emr_describe_step()
                state = hook.state_from_response(response)
                if state in self.target_states:
                    yield TriggerEvent({"status": "success", "message": f"Job flow currently {state}"})
                    return
                elif state in self.failed_states:
                    yield TriggerEvent(
                        {"status": "error", "message": hook.failure_message_from_response(response)}
                    )
                    return
                self.log.info(f"EMR step state is {state}. Sleeping for {self.poke_interval} seconds.")
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return


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
