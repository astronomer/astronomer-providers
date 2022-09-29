import asyncio
import base64
from typing import Any, Dict, Tuple, cast

import aiohttp
from aiohttp import ClientResponseError
from airflow import __version__
from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import (
    GET_RUN_ENDPOINT,
    OUTPUT_RUNS_JOB_ENDPOINT,
    DatabricksHook,
    RunState,
)
from asgiref.sync import sync_to_async

USER_AGENT_HEADER = {"user-agent": f"airflow-{__version__}"}


class DatabricksHookAsync(DatabricksHook):
    """
    Interact with Databricks.

    :param databricks_conn_id: Reference to the Databricks connection.
    :type databricks_conn_id: str
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing-out.
    :type timeout_seconds: int
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :type retry_limit: int
    :param retry_delay: The number of seconds to wait between retries (it
        might be a floating point number).
    :type retry_delay: float
    """

    async def get_run_state_async(self, run_id: str) -> RunState:
        """
        Retrieves run state of the run using an asynchronous api call.
        :param run_id: id of the run
        :return: state of the run
        """
        response = await self.get_run_response(run_id)
        state = response["state"]
        life_cycle_state = state["life_cycle_state"]
        # result_state may not be in the state if not terminal
        result_state = state.get("result_state", None)
        state_message = state["state_message"]
        self.log.info("Getting run state. ")

        return RunState(life_cycle_state, result_state, state_message)

    async def get_run_response(self, run_id: str) -> Dict[str, Any]:
        """
        Makes Async API call to get the run state info.

        :param run_id: id of the run
        """
        json = {"run_id": run_id}
        response = await self._do_api_call_async(GET_RUN_ENDPOINT, json)
        return response

    async def get_run_output_response(self, task_run_id: str) -> Dict[str, Any]:
        """
        Retrieves run output of the run.

        :param task_run_id: id of the run
        """
        json = {"run_id": task_run_id}
        run_output = await self._do_api_call_async(OUTPUT_RUNS_JOB_ENDPOINT, json)
        return run_output

    async def _do_api_call_async(
        self, endpoint_info: Tuple[str, str], json: Dict[str, Any]
    ) -> Dict[str, Any]:
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
        method, endpoint = endpoint_info
        headers = USER_AGENT_HEADER
        attempt_num = 1

        if not self.databricks_conn:
            self.databricks_conn = await sync_to_async(self.get_connection)(self.databricks_conn_id)

        if "token" in self.databricks_conn.extra_dejson:
            self.log.info("Using token auth. ")
            auth = self.databricks_conn.extra_dejson["token"]
            # aiohttp assumes basic auth for its 'auth' parameter, so we need to
            # set this manually in the header for both bearer token and basic auth.
            headers["Authorization"] = f"Bearer {auth}"
            if "host" in self.databricks_conn.extra_dejson:
                host = self._parse_host(self.databricks_conn.extra_dejson["host"])
            else:
                host = self.databricks_conn.host
        else:
            self.log.info("Using basic auth. ")
            auth_str = f"{self.databricks_conn.login}:{self.databricks_conn.password}"
            encoded_bytes = auth_str.encode("utf-8")
            auth = base64.b64encode(encoded_bytes).decode("utf-8")
            headers["Authorization"] = f"Basic {auth}"
            host = self.databricks_conn.host
            self.log.info("Auth: %s; Host: %s", auth, host)

        url = f"https://{self._parse_host(host)}/{endpoint}"
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
                        url,
                        json=json if method in ("POST", "PATCH") else None,
                        params=json if method == "GET" else None,
                        headers=headers,
                        timeout=self.timeout_seconds,
                    )
                    response.raise_for_status()
                    return cast(Dict[str, Any], await response.json())
                except ClientResponseError as e:
                    if not self._retryable_error_async(e):
                        # In this case, the user probably made a mistake.
                        # Don't retry rather raise exception
                        raise AirflowException(str(e))
                    self._log_request_error(attempt_num, str(e))

                if attempt_num == self.retry_limit:
                    raise AirflowException(
                        ("API requests to Databricks failed {} times. " + "Giving up.").format(
                            self.retry_limit
                        )
                    )

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def _retryable_error_async(self, exception: ClientResponseError) -> bool:
        """
        Determines whether or not an exception that was thrown might be successful
        on a subsequent attempt.

        Base Databricks operator considers the following to be retryable:
            - requests_exceptions.ConnectionError
            - requests_exceptions.Timeout
            - anything with a status code >= 500

        Most retryable errors are covered by status code >= 500.
        :return: if the status is retryable
        :rtype: bool
        """
        return exception.status >= 500
