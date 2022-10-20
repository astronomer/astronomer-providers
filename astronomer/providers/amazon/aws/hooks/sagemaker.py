import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from airflow.providers.amazon.aws.hooks.sagemaker import (
    LogState,
    Position,
    argmin,
    secondary_training_status_changed,
    secondary_training_status_message,
)
from asgiref.sync import sync_to_async

from astronomer.providers.amazon.aws.hooks.aws_logs import AwsLogsHookAsync
from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync
from astronomer.providers.amazon.aws.hooks.s3 import S3HookAsync


class SageMakerHookAsync(AwsBaseHookAsync):
    """
    Interact with Amazon SageMaker async using aiobotocore python library.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHookAsync.
    """

    NON_TERMINAL_STATES = ("InProgress", "Stopping", "Stopped")

    def __init__(self, *args: Any, **kwargs: Any):
        kwargs["client_type"] = "sagemaker"
        super().__init__(*args, **kwargs)
        self.s3_hook = S3HookAsync(aws_conn_id=self.aws_conn_id)
        self.logs_hook_async = AwsLogsHookAsync(aws_conn_id=self.aws_conn_id)

    async def describe_transform_job_async(self, job_name: str) -> Dict[str, Any]:
        """
        Return the transform job info associated with the name

        :param job_name: the name of the transform job
        """
        async with await self.get_client_async() as client:
            response: Dict[str, Any] = await client.describe_transform_job(TransformJobName=job_name)
            return response

    async def describe_processing_job_async(self, job_name: str) -> Dict[str, Any]:
        """
        Return the processing job info associated with the name

        :param job_name: the name of the processing job
        """
        async with await self.get_client_async() as client:
            response: Dict[str, Any] = await client.describe_processing_job(ProcessingJobName=job_name)
            return response

    async def describe_training_job_async(self, job_name: str) -> Dict[str, Any]:
        """
        Return the training job info associated with the name

        :param job_name: the name of the training job
        """
        async with await self.get_client_async() as client:
            response: Dict[str, Any] = await client.describe_training_job(TrainingJobName=job_name)
            return response

    async def describe_training_job_with_log(
        self,
        job_name: str,
        positions: Dict[str, Any],
        stream_names: List[str],
        instance_count: int,
        state: int,
        last_description: Dict[str, Any],
        last_describe_job_call: float,
    ) -> Tuple[int, Dict[str, Any], float]:
        """
        Return the training job info associated with job_name and print CloudWatch logs

        :param job_name: name of the job to check status
        :param positions: A list of pairs of (timestamp, skip) which represents the last record
            read from each stream.
        :param stream_names: A list of the log stream names. The position of the stream in this list is
            the stream number.
        :param instance_count: Count of the instance created for the job initially
        :param state: log state
        :param last_description: Latest description of the training job
        :param last_describe_job_call: previous job called time
        """
        log_group = "/aws/sagemaker/TrainingJobs"

        if len(stream_names) < instance_count:
            streams = await self.logs_hook_async.describe_log_streams_async(
                log_group=log_group,
                stream_prefix=job_name + "/",
                order_by="LogStreamName",
                count=instance_count,
            )
            stream_names = [s["logStreamName"] for s in streams["logStreams"]] if streams else []
            positions.update([(s, Position(timestamp=0, skip=0)) for s in stream_names if s not in positions])

        if len(stream_names) > 0:
            async for idx, event in self.get_multi_stream(log_group, stream_names, positions):
                self.log.info(event["message"])
                ts, count = positions[stream_names[idx]]
                if event["timestamp"] == ts:
                    positions[stream_names[idx]] = Position(timestamp=ts, skip=count + 1)  # pragma: no cover
                else:
                    positions[stream_names[idx]] = Position(timestamp=event["timestamp"], skip=1)

        if state == LogState.COMPLETE:
            return state, last_description, last_describe_job_call

        if state == LogState.JOB_COMPLETE:
            state = LogState.COMPLETE
        elif time.time() - last_describe_job_call >= 30:
            description = await self.describe_training_job_async(job_name)
            last_describe_job_call = time.time()

            if await sync_to_async(secondary_training_status_changed)(description, last_description):
                self.log.info(
                    await sync_to_async(secondary_training_status_message)(description, last_description)
                )  # pragma: no cover
                last_description = description  # pragma: no cover

            status = description["TrainingJobStatus"]

            if status not in self.NON_TERMINAL_STATES:
                state = LogState.JOB_COMPLETE
        return state, last_description, last_describe_job_call

    async def get_multi_stream(
        self, log_group: str, streams: List[str], positions: Dict[str, Any]
    ) -> AsyncGenerator[Any, Tuple[int, Optional[Any]]]:
        """
        Iterate over the available events coming from a set of log streams in a single log group
        interleaving the events from each stream so they're yielded in timestamp order.

        :param log_group: The name of the log group.
        :param streams: A list of the log stream names. The position of the stream in this list is
            the stream number.
        :param positions: A list of pairs of (timestamp, skip) which represents the last record
            read from each stream.
        """
        positions = positions or {s: Position(timestamp=0, skip=0) for s in streams}
        events: list[Optional[Any]] = []

        event_iters = [
            self.logs_hook_async.get_log_events_async(log_group, s, positions[s].timestamp, positions[s].skip)
            for s in streams
        ]
        for event_stream in event_iters:
            if not event_stream:
                events.append(None)  # pragma: no cover
                continue  # pragma: no cover
            try:
                events.append(await event_stream.__anext__())
            except StopAsyncIteration:  # pragma: no cover
                events.append(None)  # pragma: no cover

        while any(events):
            i = argmin(events, lambda x: x["timestamp"] if x else 9999999999) or 0
            yield i, events[i]
            try:
                events[i] = await event_iters[i].__anext__()
            except StopAsyncIteration:
                events[i] = None
