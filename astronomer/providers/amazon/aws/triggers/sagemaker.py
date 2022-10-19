import asyncio
import time
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync


class SagemakerProcessingTrigger(BaseTrigger):
    """
    SagemakerProcessingTrigger is fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param poll_interval:  polling period in seconds to check for the status
    :param aws_conn_id: AWS connection ID for sagemaker
    :param end_time: the end time in seconds. Any
            SageMaker jobs that run longer than this will fail.
    """

    NON_TERMINAL_STATES = ("InProgress", "Stopping")
    TERMINAL_STATE = ("Failed",)

    def __init__(
        self,
        job_name: str,
        poll_interval: float,
        end_time: Optional[float],
        aws_conn_id: str = "aws_default",
    ):
        super().__init__()
        self.job_name = job_name
        self.poll_interval = poll_interval
        self.aws_conn_id = aws_conn_id
        self.end_time = end_time

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SagemakerProcessingTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerProcessingTrigger",
            {
                "job_name": self.job_name,
                "poll_interval": self.poll_interval,
                "aws_conn_id": self.aws_conn_id,
                "end_time": self.end_time,
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
                # check if time limit is set and timeout has happened or not
                if self.end_time and time.time() > self.end_time:
                    yield TriggerEvent({"status": "error", "message": "Timeout"})
                response = await hook.describe_processing_job_async(self.job_name)
                status = response["ProcessingJobStatus"]
                if status in self.TERMINAL_STATE:
                    error_message = f"SageMaker job failed because {response['FailureReason']}"
                    yield TriggerEvent({"status": "error", "message": error_message})
                elif status in self.NON_TERMINAL_STATES:
                    self.log.info("Job still running current status is %s", status)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "success", "message": response})
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> SageMakerHookAsync:
        return SageMakerHookAsync(aws_conn_id=self.aws_conn_id)
