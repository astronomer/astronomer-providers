import asyncio
import base64

import aiohttp
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import (
    GET_RUN_ENDPOINT,
    USER_AGENT_HEADER,
    DatabricksHook,
    RunState,
)
from asgiref.sync import sync_to_async

DEFAULT_CONN_NAME = "databricks_default"


class DatabricksHookAsync(DatabricksHook):
    def __init__(
        self,
        databricks_conn_id: str = DEFAULT_CONN_NAME,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self.databricks_conn_id = databricks_conn_id
        self.databricks_conn = None  # To be set asynchronously in create_hook()
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def get_run_state_async(self, run_id: str) -> RunState:
        """
        Retrieves run state of the run using an asyncronous api call.
        :param run_id: id of the run
        :return: state of the run
        """
        json = {"run_id": run_id}
        response = await self._do_api_call_async(GET_RUN_ENDPOINT, json)
        state = response["state"]
        life_cycle_state = state["life_cycle_state"]
        # result_state may not be in the state if not terminal
        result_state = state.get("result_state", None)
        state_message = state["state_message"]
        self.log.info("Getting run state. ")

        return RunState(life_cycle_state, result_state, state_message)

    async def _do_api_call_async(self, endpoint_info, json):
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
            headers["Authorization"] = f"Basic: {auth}"
            host = self.databricks_conn.host
            self.log.info(f"auth: {auth}")
            self.log.info(f"host: {host}")

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
                    return await response.json()
                except ClientResponseError as e:
                    if not self._retryable_error_async(e):
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        raise AirflowException(
                            f"Response: {e.message}, Status Code: {e.status}"
                        )
                    self._log_request_error(attempt_num, e)

                if attempt_num == self.retry_limit:
                    raise AirflowException(
                        (
                            "API requests to Databricks failed {} times. "
                            + "Giving up."
                        ).format(self.retry_limit)
                    )

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def _retryable_error_async(self, exception) -> bool:
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


async def create_hook():
    """
    Initializes a new DatabricksHookAsync then sets its databricks_conn
    field asynchronously.
    :return: a new async Databricks hook
    :rtype: DataBricksHookAsync()
    """
    self = DatabricksHookAsync()
    self.databricks_conn = await sync_to_async(self.get_connection)(
        self.databricks_conn_id
    )
    return self
