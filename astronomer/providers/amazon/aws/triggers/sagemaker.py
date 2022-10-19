import asyncio
import time
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from airflow.providers.amazon.aws.hooks.sagemaker import LogState
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.sagemaker import SageMakerHookAsync


class SagemakerTrigger(BaseTrigger):
    """
    SagemakerTrigger is common trigger for both transform and training sagemaker job and it is
     fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param job_type: Type of the sagemaker job whether it is Transform or Training
    :param response_key: The key which needs to be look in the response.
    :param poke_interval:  polling period in seconds to check for the status
    :param end_time: Time in seconds to wait for a job run to reach a terminal status.
    :param aws_conn_id: AWS connection ID for sagemaker
    """

    non_terminal_states = {"InProgress", "Stopping", "Stopped"}
    failed_states = {"Failed"}

    def __init__(
        self,
        job_name: str,
        job_type: str,
        response_key: str,
        poke_interval: float,
        end_time: Optional[float] = None,
        aws_conn_id: str = "aws_default",
    ):
        super().__init__()
        self.job_name = job_name
        self.job_type = job_type
        self.response_key = response_key
        self.poke_interval = poke_interval
        self.end_time = end_time
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SagemakerTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrigger",
            {
                "poke_interval": self.poke_interval,
                "aws_conn_id": self.aws_conn_id,
                "end_time": self.end_time,
                "job_name": self.job_name,
                "job_type": self.job_type,
                "response_key": self.response_key,
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
                response = await self.get_job_status(hook, self.job_name, self.job_type)
                status = response[self.response_key]
                if status in self.non_terminal_states:
                    await asyncio.sleep(self.poke_interval)
                elif status in self.failed_states:
                    error_message = f"SageMaker job failed because {response['FailureReason']}"
                    yield TriggerEvent({"status": "error", "message": error_message})
                else:
                    yield TriggerEvent({"status": "success", "message": "SageMaker Job completed"})
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> SageMakerHookAsync:
        return SageMakerHookAsync(aws_conn_id=self.aws_conn_id)

    @staticmethod
    async def get_job_status(hook: SageMakerHookAsync, job_name: str, job_type: str) -> Dict[str, Any]:
        """
        Based on the job type the SageMakerHookAsync connect to sagemaker related function
        and get the response of the job and return it
        """
        if job_type == "Transform":
            response = await hook.describe_transform_job_async(job_name)
        elif job_type == "Training":
            response = await hook.describe_training_job_async(job_name)
        return response


class SagemakerTrainingWithLogTrigger(BaseTrigger):
    """
    SagemakerTrainingWithLogTrigger is fired as deferred class with params to run the task in triggerer.

    :param job_name: name of the job to check status
    :param instance_count: count of the instance created for running the training job
    :param status: The status of the training job created.
    :param poke_interval:  polling period in seconds to check for the status
    :param end_time: Time in seconds to wait for a job run to reach a terminal status.
    :param aws_conn_id: AWS connection ID for sagemaker
    """

    non_terminal_states = {"InProgress", "Stopping", "Stopped"}
    failed_states = {"Failed"}

    def __init__(
        self,
        job_name: str,
        instance_count: int,
        status: str,
        poke_interval: float,
        end_time: Optional[float] = None,
        aws_conn_id: str = "aws_default",
    ):
        super().__init__()
        self.job_name = job_name
        self.instance_count = instance_count
        self.status = status
        self.poke_interval = poke_interval
        self.end_time = end_time
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SagemakerTrainingWithLogTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.sagemaker.SagemakerTrainingWithLogTrigger",
            {
                "poke_interval": self.poke_interval,
                "aws_conn_id": self.aws_conn_id,
                "end_time": self.end_time,
                "job_name": self.job_name,
                "status": self.status,
                "instance_count": self.instance_count,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes async connection to sagemaker async hook and gets job status for a job submitted by the operator.
        Trigger returns a failure event if any error and success in state return the success event.
        """
        hook = self._get_async_hook()

        last_description = await hook.describe_training_job_async(self.job_name)
        stream_names: List[str] = []  # The list of log streams
        positions: Dict[str, Any] = {}  # The current position in each stream, map of stream name -> position

        job_already_completed = self.status not in self.non_terminal_states

        print("job_already_completed ", job_already_completed)
        state = LogState.TAILING if not job_already_completed else LogState.COMPLETE
        print("state 11111 ", state)
        last_describe_job_call = time.time()
        while True:
            print("999999999")
            try:
                print("8888888")
                if self.end_time and time.time() > self.end_time:
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"SageMaker job took more than {self.end_time} seconds",
                        }
                    )
                    print("999999 8888888")
                state, last_description, last_describe_job_call = await hook.describe_training_job_with_log(
                    self.job_name,
                    positions,
                    stream_names,
                    self.instance_count,
                    state,
                    last_description,
                    last_describe_job_call,
                )
                print("777777777")
                status = last_description["TrainingJobStatus"]
                print("status=========", status)
                print("status in self.non_terminal_states ", status in self.non_terminal_states)
                if status in self.non_terminal_states:
                    print("88888888 -------->")
                    await asyncio.sleep(self.poke_interval)
                elif status in self.failed_states:
                    reason = last_description.get("FailureReason", "(No reason provided)")
                    error_message = f"SageMaker job failed because {reason}"
                    yield TriggerEvent({"status": "error", "message": error_message})
                else:
                    billable_time = (
                        last_description["TrainingEndTime"] - last_description["TrainingStartTime"]
                    ) * self.instance_count
                    self.log.info("Billable seconds: %d", int(billable_time.total_seconds()) + 1)
                    yield TriggerEvent({"status": "success", "message": "SageMaker Job completed"})
            except Exception as e:
                yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> SageMakerHookAsync:
        return SageMakerHookAsync(aws_conn_id=self.aws_conn_id)
