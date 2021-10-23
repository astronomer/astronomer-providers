import asyncio
import aiohttp
import time
from aiohttp import ClientResponseError
from asgiref.sync import sync_to_async
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook


DEFAULT_CONN_NAME = "airbyte_default"


class AirbyteHookAsync(AirbyteHook):
    def __init__(
        self,
        airbyte_conn_id: str = DEFAULT_CONN_NAME,
        timeout: int = 180,
        wait_seconds: int = 3,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        super().__init__(airbyte_conn_id=airbyte_conn_id)
        self.airbyte_conn = None  # To be set asynchronously in create_hook()
        self.wait_seconds = wait_seconds
        self.timeout = timeout
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def get_run_state_async(self, job_id: str) -> dict:
        """
        Retrieves run state of the run using an asyncronous api call.
        :param job_id: id of the run
        :return: state of the run
        """
        state = None
        start = time.monotonic()
        while True:
            if self.timeout and start + self.timeout < time.monotonic():
                raise AirflowException(f"Timeout: Airbyte job {job_id} is not ready after {self.timeout}s")
            await asyncio.sleep(self.wait_seconds)
            return await self.get_job_async(job_id=job_id)

    async def get_job_async(self, job_id: int) -> Any:
        """
        Gets the resource representation for a job in Airbyte.

        :param job_id: Required. Id of the Airbyte job
        :type job_id: int
        """
        self.airbyte_conn = await sync_to_async(self.get_connection)(self.http_conn_id)
        return await self._do_api_call_async(
            method="POST",
            endpoint=f"{self.airbyte_conn.host}/api/{self.api_version}/jobs/get",
            json={"id": job_id},
            headers={"accept": "application/json"},
        )

    async def _do_api_call_async(self, method, endpoint, json, headers={}):
        """
        Utility function to perform an asynchronous API call with retries
        :param endpoint_info: Tuple of method and endpoint
        :type endpoint_info: tuple[string, string]
        :param json: Parameters for this API call.
        :type json: dict
        :return: If the api call returns a OK status code,
            this function returns the response in JSON. Otherwise,
            we throw an AirflowException.
        :rtype: dict
        """
        attempt_num = 1

        async with aiohttp.ClientSession() as session:
            if method == "GET":
                request_func = session.get
            elif method == "POST":
                request_func = session.post
            elif method == "PATCH":
                request_func = session.patch
            else:
                raise AirflowException("Unexpected HTTP Method: " + method)

            while True:
                try:
                    response = await request_func(
                        endpoint,
                        json=json if method in ("POST", "PATCH") else None,
                        params=json if method == "GET" else None,
                        headers=headers,
                        timeout=self.timeout,
                    )
                    response.raise_for_status()
                    return await response.json()
                except ClientResponseError as e:
                    self.log.error('Attempt %s API Request to Airbyte failed with reason: %s', attempt_num, e)

                if attempt_num == self.retry_limit:
                    raise AirflowException(
                        (
                            "API requests to Airbyte failed {} times. "
                            + "Giving up."
                        ).format(self.retry_limit)
                    )

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)
