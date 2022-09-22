import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.batch_client import BatchClientHookAsync


class BatchOperatorTrigger(BaseTrigger):
    """
    Checks for the state of a previously submitted job to AWS Batch.
    BatchOperatorTrigger is fired as deferred class with params to poll the job state in Triggerer

    :param job_id: the job ID, usually unknown (None) until the
        submit_job operation gets the jobId defined by AWS Batch
    :param job_name: the name for the job that will run on AWS Batch (templated)
    :param job_definition: the job definition name on AWS Batch
    :param job_queue: the queue name on AWS Batch
    :param overrides: the `containerOverrides` parameter for boto3 (templated)
    :param array_properties: the `arrayProperties` parameter for boto3
    :param parameters: the `parameters` for boto3 (templated)
    :param waiters: a :class:`.BatchWaiters` object (see note below);
        if None, polling is used with max_retries and status_retries.
    :param tags: collection of tags to apply to the AWS Batch job submission
        if None, no tags are submitted
    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.
    :param region_name: AWS region name to use .
        Override the region_name in connection (if provided)
    """

    def __init__(
        self,
        job_id: Optional[str],
        job_name: str,
        job_definition: str,
        job_queue: str,
        overrides: Dict[str, str],
        array_properties: Dict[str, str],
        parameters: Dict[str, str],
        waiters: Any,
        tags: Dict[str, str],
        max_retries: int,
        status_retries: int,
        region_name: Optional[str],
        aws_conn_id: Optional[str] = "aws_default",
    ):
        super().__init__()
        self.job_id = job_id
        self.job_name = job_name
        self.job_definition = job_definition
        self.job_queue = job_queue
        self.overrides = overrides or {}
        self.array_properties = array_properties or {}
        self.parameters = parameters or {}
        self.waiters = waiters
        self.tags = tags or {}
        self.max_retries = max_retries
        self.status_retries = status_retries
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BatchOperatorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.batch.BatchOperatorTrigger",
            {
                "job_id": self.job_id,
                "job_name": self.job_name,
                "job_definition": self.job_definition,
                "job_queue": self.job_queue,
                "overrides": self.overrides,
                "array_properties": self.array_properties,
                "parameters": self.parameters,
                "waiters": self.waiters,
                "tags": self.tags,
                "max_retries": self.max_retries,
                "status_retries": self.status_retries,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Make async connection using aiobotocore library to AWS Batch,
        periodically poll for the job status on the Triggerer

        The status that indicates job completion are: 'SUCCEEDED'|'FAILED'.

        So the status options that this will poll for are the transitions from:
        'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'>'SUCCEEDED'|'FAILED'
        """
        hook = BatchClientHookAsync(job_id=self.job_id, waiters=self.waiters, aws_conn_id=self.aws_conn_id)
        try:
            response = await hook.monitor_job()
            if response:
                yield TriggerEvent(response)
            else:
                error_message = f"{self.job_id} failed"
                yield TriggerEvent({"status": "error", "message": error_message})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})


class BatchSensorTrigger(BaseTrigger):
    """
    Checks for the status of a submitted job_id to AWS Batch until it reaches a failure or a success state.
    BatchSensorTrigger is fired as deferred class with params to poll the job state in Triggerer

    :param job_id: the job ID, to poll for job completion or not
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
    :param region_name: AWS region name to use
        Override the region_name in connection (if provided)
    :param poke_interval: polling period in seconds to check for the status of the job
    """

    def __init__(
        self,
        job_id: str,
        region_name: Optional[str],
        aws_conn_id: Optional[str] = "aws_default",
        poke_interval: float = 5,
    ):
        super().__init__()
        self.job_id = job_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.poke_interval = poke_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BatchSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.batch.BatchSensorTrigger",
            {
                "job_id": self.job_id,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Make async connection using aiobotocore library to AWS Batch,
        periodically poll for the Batch job status

        The status that indicates job completion are: 'SUCCEEDED'|'FAILED'.
        """
        hook = BatchClientHookAsync(job_id=self.job_id, aws_conn_id=self.aws_conn_id)
        try:
            while True:
                response = await hook.get_job_description(self.job_id)
                state = response["status"]
                if state == BatchClientHookAsync.SUCCESS_STATE:
                    success_message = f"{self.job_id} was completed successfully"
                    yield TriggerEvent({"status": "success", "message": success_message})
                if state == BatchClientHookAsync.FAILURE_STATE:
                    error_message = f"{self.job_id} failed"
                    yield TriggerEvent({"status": "error", "message": error_message})
                await asyncio.sleep(self.poke_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
