import asyncio
import warnings
from typing import Any, AsyncIterator, Dict, Optional, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.http.hooks.http import HttpHookAsync


class HttpTrigger(BaseTrigger):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.http.triggers.http.HttpSensorTrigger` instead.
    """

    def __init__(
        self,
        endpoint: str,
        http_conn_id: str = "http_default",
        method: str = "GET",
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        poke_interval: float = 5.0,
    ):
        warnings.warn(
            (
                "This class is deprecated. "
                "Use `~airflow.providers.http.triggers.http.HttpSensorTrigger` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__()
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.extra_options = extra_options or {}
        self.http_conn_id = http_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes HttpTrigger arguments and classpath."""
        return (
            "astronomer.providers.http.triggers.http.HttpTrigger",
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
        Makes a series of asynchronous http calls via an http hook. It yields a Trigger if
        response is a 200 and run_state is successful, will retry the call up to the retry limit
        if the error is 'retryable', otherwise it throws an exception.
        """
        hook = self._get_async_hook()
        while True:
            try:
                await hook.run(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    extra_options=self.extra_options,
                )
                yield TriggerEvent(True)
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poke_interval)

    def _get_async_hook(self) -> HttpHookAsync:
        return HttpHookAsync(
            method=self.method,
            http_conn_id=self.http_conn_id,
        )
