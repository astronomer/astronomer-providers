import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ExternalDeploymentTaskTrigger(BaseTrigger):
    """
    Makes HTTP calls to an external deployment's Airflow API and polls for the state of a task.

    :param endpoint: The relative part of the full url
    :param http_conn_id: The Connection ID to run the trigger against
    :param method: The HTTP request method to use
    :param data: The parameters to be added to the GET url
    :param headers: The HTTP headers to be added to the GET request
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param poke_interval: Time in seconds to wait between each API call
    """

    def __init__(
        self,
        endpoint: str,
        http_conn_id: str = "http_default",
        method: str = "GET",
        data: dict[str, Any] | str | None = None,
        headers: dict[str, Any] | None = None,
        extra_options: dict[str, Any] | None = None,
        poke_interval: float = 5.0,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.extra_options = extra_options or {}
        self.http_conn_id = http_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes ExternalDeploymentTaskTrigger arguments and classpath."""
        return (
            "astronomer.providers.core.triggers.external_task.ExternalDeploymentTaskTrigger",
            {
                "endpoint": self.endpoint,
                "data": self.data,
                "headers": self.headers,
                "extra_options": self.extra_options,
                "http_conn_id": self.http_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """
        Makes a series of http calls via an http hook poll for state of the job
        run until it reaches a failure state or success state. It yields a Trigger if response state is successful.
        """
        from airflow.utils.state import State

        hook = HttpHook(method="GET", http_conn_id=self.http_conn_id)
        while True:
            try:
                response = hook.run(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    extra_options=self.extra_options,
                )
                resp_json = response.json()
                if resp_json["state"] in State.finished:
                    yield TriggerEvent(resp_json)
                    return
                self.log.info(
                    "The current status is %s. Sleeping for %s seconds",
                    resp_json.get("state"),
                    self.poke_interval,
                )
                await asyncio.sleep(self.poke_interval)
            except AirflowException as exc:
                self.log.info("An error occur while calling API %s", str(exc))
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poke_interval)
                yield TriggerEvent({"state": "error", "message": str(exc)})
                return
