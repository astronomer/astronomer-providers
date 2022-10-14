import asyncio
import time
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync


class SagemakerTransformTrigger(BaseTrigger):
    """
    SagemakerTransformTrigger is fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param poll_interval:  polling period in seconds to check for the status
    :param end_time: Time in seconds to wait for a job run to reach a terminal status.
    :param aws_conn_id: AWS connection ID for sagemaker
    """

    non_terminal_states = {"InProgress", "Stopping", "Stopped"}
    failed_states = {"Failed"}

    def __init__(
        self,
        job_name: str,
        poll_interval: float,
        end_time: Optional[float] = None,
        aws_conn_id: str = "aws_default",
    ):
        super().__init__()
        self.job_name = job_name
        self.poll_interval = poll_interval
        self.end_time = end_time
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SagemakerTransformTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTransformTrigger",
            {
                "poll_interval": self.poll_interval,
                "aws_conn_id": self.aws_conn_id,
                "end_time": self.end_time,
                "job_name": self.job_name,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes async connection to sagemaker async hook and gets job status for a job submitted by the operator.
        Trigger returns a failure event if any error and success in state return the success event.
        """
        hook = self._get_async_hook()
        while True:
            try:
                if self.end_time and time.time() > self.end_time:
                    yield TriggerEvent({"status": "error", "message": "Timeout"})
                response = await hook.describe_transform_job_async(self.job_name)
                status = response["TransformJobStatus"]
                if status in self.non_terminal_states:
                    await asyncio.sleep(self.poll_interval)
                elif status in self.failed_states:
                    error_message = f"SageMaker job failed because {response['FailureReason']}"
                    yield TriggerEvent({"status": "error", "message": error_message})
                else:
                    yield TriggerEvent({"status": "success", "message": "SageMaker Job completed"})
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> SageMakerHookAsync:
        return SageMakerHookAsync(aws_conn_id=self.aws_conn_id)


class SagemakerTrainingTrigger(SagemakerTransformTrigger):
    """
    SagemakerTrainingTrigger is fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param poll_interval:  polling period in seconds to check for the status
    :param end_time: Time in seconds to wait for a job run to reach a terminal status.
    :param aws_conn_id: AWS connection ID for sagemaker
    """

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SagemakerTrainingTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrainingTrigger",
            {
                "poll_interval": self.poll_interval,
                "aws_conn_id": self.aws_conn_id,
                "end_time": self.end_time,
                "job_name": self.job_name,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes async connection to sagemaker async hook and gets job status for a job submitted by the operator.
        Trigger returns a failure event if any error and success in state return the success event.
        """
        hook = self._get_async_hook()
        while True:
            try:
                if self.end_time and time.time() > self.end_time:
                    yield TriggerEvent({"status": "error", "message": "Timeout"})
                response = await hook.describe_training_job_async(self.job_name)
                status = response["TransformJobStatus"]
                if status in self.non_terminal_states:
                    await asyncio.sleep(self.poll_interval)
                elif status in self.failed_states:
                    error_message = f"SageMaker job failed because {response['FailureReason']}"
                    yield TriggerEvent({"status": "error", "message": error_message})
                else:
                    yield TriggerEvent({"status": "success", "message": "SageMaker Job completed"})
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})
