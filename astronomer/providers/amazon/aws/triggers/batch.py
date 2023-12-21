from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.amazon.aws.hooks.batch_client import BatchClientHookAsync


class BatchOperatorTrigger(BaseTrigger):
    """
    Checks for the state of a previously submitted job to AWS Batch.
    BatchOperatorTrigger is fired as deferred class with params to poll the job state in Triggerer

    :param job_id: the job ID, usually unknown (None) until the
        submit_job operation gets the jobId defined by AWS Batch
    :param waiters: a :class:`.BatchWaiters` object (see note below);
        if None, polling is used with max_retries and status_retries.
    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.
    :param region_name: AWS region name to use .
        Override the region_name in connection (if provided)
    """

    def __init__(
        self,
        job_id: Optional[str],
        waiters: Any,
        max_retries: int,
        region_name: Optional[str],
        aws_conn_id: Optional[str] = "aws_default",
    ):
        super().__init__()
        self.job_id = job_id
        self.waiters = waiters
        self.max_retries = max_retries
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BatchOperatorTrigger arguments and classpath."""
        return (
            "astronomer.providers.amazon.aws.triggers.batch.BatchOperatorTrigger",
            {
                "job_id": self.job_id,
                "waiters": self.waiters,
                "max_retries": self.max_retries,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
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
