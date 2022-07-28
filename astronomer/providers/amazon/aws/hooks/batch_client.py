import asyncio
from random import sample
from typing import Any, Dict, List, Optional, Union

import botocore.exceptions
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook

from astronomer.providers.amazon.aws.hooks.base_aws import AwsBaseHookAsync


class BatchClientHookAsync(BatchClientHook, AwsBaseHookAsync):
    """
    Async client for AWS Batch services.

    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None

    .. note::
        Several methods use a default random delay to check or poll for job status, i.e.
        ``random.sample()``
        Using a random interval helps to avoid AWS API throttle limits
        when many concurrent tasks request job-descriptions.

        To modify the global defaults for the range of jitter allowed when a
        random delay is used to check Batch job status, modify these defaults, e.g.:

            BatchClient.DEFAULT_DELAY_MIN = 0
            BatchClient.DEFAULT_DELAY_MAX = 5

        When explicit delay values are used, a 1 second random jitter is applied to the
        delay .  It is generally recommended that random jitter is added to API requests.
        A convenience method is provided for this, e.g. to get a random delay of
        10 sec +/- 5 sec: ``delay = BatchClient.add_jitter(10, width=5, minima=0)``

    .. seealso::
        - `Batch <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch.html>`_
        - `Retries <https://docs.aws.amazon.com/general/latest/gr/api-retries.html>`_
        - `Exponential Backoff And Jitter <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>`_
    """

    def __init__(self, job_id: Optional[str], waiters: Any = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.job_id = job_id
        self.waiters = waiters

    async def monitor_job(self) -> Union[Dict[str, str], None]:
        """
        Monitor an AWS Batch job
        monitor_job can raise an exception or an AirflowTaskTimeout can be raised if execution_timeout
        is given while creating the task. These exceptions should be handled in taskinstance.py
        instead of here like it was previously done

        :raises: AirflowException
        """
        if not self.job_id:
            raise AirflowException("AWS Batch job - job_id was not found")

        if self.waiters:
            self.waiters.wait_for_job(self.job_id)
            return None
        else:
            await self.wait_for_job(self.job_id)
            await self.check_job_success(self.job_id)
            success_msg = "AWS Batch job (%s) succeeded" % self.job_id
            self.log.info(success_msg)
            return {"status": "success", "message": success_msg}

    async def check_job_success(self, job_id: str) -> bool:  # type: ignore[override]
        """
        Check the final status of the Batch job; return True if the job
        'SUCCEEDED', else raise an AirflowException

        :param job_id: a Batch job ID

        :raises: AirflowException
        """
        job = await self.get_job_description(job_id)
        job_status = job.get("status")
        if job_status == self.SUCCESS_STATE:
            self.log.info("AWS Batch job (%s) succeeded: %s", job_id, job)
            return True

        if job_status == self.FAILURE_STATE:
            raise AirflowException(f"AWS Batch job ({job_id}) failed: {job}")

        if job_status in self.INTERMEDIATE_STATES:
            raise AirflowException(f"AWS Batch job ({job_id}) is not complete: {job}")

        raise AirflowException(f"AWS Batch job ({job_id}) has unknown status: {job}")

    @staticmethod
    async def delay(delay: Union[int, float, None] = None) -> None:  # type: ignore[override]
        """
        Pause execution for ``delay`` seconds.

        :param delay: a delay to pause execution using ``time.sleep(delay)``;
            a small 1 second jitter is applied to the delay.

        .. note::
            This method uses a default random delay, i.e.
            ``random.sample()``;
            using a random interval helps to avoid AWS API throttle limits
            when many concurrent tasks request job-descriptions.
        """
        if delay is None:
            """Using random.uniform() causes Bandit security check to fail with Issue: [B311:blacklist] Standard
            pseudo-random generators are not suitable for security/cryptographic purposes. Hence,
            using random.sample() instead"""
            delay = sample(
                list(range(BatchClientHookAsync.DEFAULT_DELAY_MIN, BatchClientHookAsync.DEFAULT_DELAY_MAX)), 1
            )[0]
        else:
            delay = BatchClientHookAsync.add_jitter(delay)
        await asyncio.sleep(delay)

    async def wait_for_job(self, job_id: str, delay: Union[int, float, None] = None) -> None:  # type: ignore[override]
        """
        Wait for Batch job to complete

        :param job_id: a Batch job ID
        :param delay: a delay before polling for job status

        :raises: AirflowException
        """
        await self.delay(delay)
        await self.poll_for_job_running(job_id, delay)
        await self.poll_for_job_complete(job_id, delay)
        self.log.info("AWS Batch job (%s) has completed", job_id)

    async def poll_for_job_complete(  # type: ignore[override]
        self, job_id: str, delay: Union[int, float, None] = None
    ) -> None:
        """
        Poll for job completion. The status that indicates job completion
        are: 'SUCCEEDED'|'FAILED'.

        So the status options that this will wait for are the transitions from:
        'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'>'SUCCEEDED'|'FAILED'

        :param job_id: a Batch job ID
        :param delay: a delay before polling for job status

        :raises: AirflowException
        """
        await self.delay(delay)
        complete_status = [self.SUCCESS_STATE, self.FAILURE_STATE]
        await self.poll_job_status(job_id, complete_status)

    async def poll_for_job_running(  # type: ignore[override]
        self, job_id: str, delay: Union[int, float, None] = None
    ) -> None:
        """
        Poll for job running. The status that indicates a job is running or
        already complete are: 'RUNNING'|'SUCCEEDED'|'FAILED'.

        So the status options that this will wait for are the transitions from:
        'SUBMITTED'>'PENDING'>'RUNNABLE'>'STARTING'>'RUNNING'|'SUCCEEDED'|'FAILED'

        The completed status options are included for cases where the status
        changes too quickly for polling to detect a RUNNING status that moves
        quickly from STARTING to RUNNING to completed (often a failure).

        :param job_id: a Batch job ID
        :param delay: a delay before polling for job status

        :raises: AirflowException
        """
        await self.delay(delay)
        running_status = [self.RUNNING_STATE, self.SUCCESS_STATE, self.FAILURE_STATE]
        await self.poll_job_status(job_id, running_status)

    async def get_job_description(self, job_id: str) -> Dict[str, str]:  # type: ignore[override]
        """
        Get job description (using status_retries).

        :param job_id: a Batch job ID
        :raises: AirflowException
        """
        retries = 0
        async with await self.get_client_async() as client:
            while True:
                try:
                    response = await client.describe_jobs(jobs=[job_id])
                    return self.parse_job_description(job_id, response)

                except botocore.exceptions.ClientError as err:
                    error = err.response.get("Error", {})
                    if error.get("Code") == "TooManyRequestsException":
                        pass  # allow it to retry, if possible
                    else:
                        raise AirflowException(f"AWS Batch job ({job_id}) description error: {err}")

                retries += 1
                if retries >= self.status_retries:
                    raise AirflowException(
                        f"AWS Batch job ({job_id}) description error: exceeded status_retries "
                        f"({self.status_retries})"
                    )

                pause = self.exponential_delay(retries)
                self.log.info(
                    "AWS Batch job (%s) description retry (%d of %d) in the next %.2f seconds",
                    job_id,
                    retries,
                    self.status_retries,
                    pause,
                )
                await self.delay(pause)

    async def poll_job_status(self, job_id: str, match_status: List[str]) -> bool:  # type: ignore[override]
        """
        Poll for job status using an exponential back-off strategy (with max_retries).
        The Batch job status polled are:
        'SUBMITTED'|'PENDING'|'RUNNABLE'|'STARTING'|'RUNNING'|'SUCCEEDED'|'FAILED'

        :param job_id: a Batch job ID
        :param match_status: a list of job status to match
        :raises: AirflowException
        """
        retries = 0
        while True:
            job = await self.get_job_description(job_id)
            job_status = job.get("status")
            self.log.info(
                "AWS Batch job (%s) check status (%s) in %s",
                job_id,
                job_status,
                match_status,
            )
            if job_status in match_status:
                return True

            if retries >= self.max_retries:
                raise AirflowException(f"AWS Batch job ({job_id}) status checks exceed max_retries")

            retries += 1
            pause = self.exponential_delay(retries)
            self.log.info(
                "AWS Batch job (%s) status check (%d of %d) in the next %.2f seconds",
                job_id,
                retries,
                self.max_retries,
                pause,
            )
            await self.delay(pause)
