import asyncio
from typing import Any, Dict, Optional, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.providers.http.sensors.http import HttpSensor
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.hooks.http import HttpHookAsync


class HttpSensorAsync(HttpSensor):
    def execute(self, context):
        """
        Logic that the sensor uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
        # TODO: We can't currently serialize arbitrary function
        # Maybe we set method_name as users function??? to run it again
        # and evaluate the response.
        if self.response_check:
            super().execute(context=context)
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=HttpTrigger(
                    method=self.hook.method,  # TODO: Fix this to directly get method from ctor
                    endpoint=self.endpoint,
                    data=self.request_params,
                    headers=self.headers,
                    extra_options=self.extra_options,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None


class HttpTrigger(BaseTrigger):
    """
    A trigger that fires when the request to a URL returns a non-404 status code

    :param endpoint: The relative part of the full url
    :type endpoint: str
    :param http_conn_id: The :ref:`http connection<howto/connection:http>` to run the
        sensor against
    :type http_conn_id: str
    :param method: The HTTP request method to use
    :type method: str
    :param data: payload to be uploaded or aiohttp parameters
    :type data: dict
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param extra_options: Additional kwargs to pass when creating a request.
        For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
    :type extra_options: dict
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    :param poll_interval: Time to sleep using asyncio
    :type poll_interval: float
    """

    def __init__(
        self,
        endpoint: str,
        http_conn_id: str = "http_default",
        method: str = "GET",
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.endpoint = endpoint
        self.method = method
        self.data = data
        self.headers = headers
        self.extra_options = extra_options or {}
        self.http_conn_id = http_conn_id
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes HttpTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.http.HttpTrigger",
            {
                "endpoint": self.endpoint,
                "data": self.data,
                "headers": self.headers,
                "extra_options": self.extra_options,
                "http_conn_id": self.http_conn_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        """
        Makes a series of asynchronous http calls via a Databrick hook. It yields a Trigger if
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
                return
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poll_interval)

    def _get_async_hook(self) -> HttpHookAsync:
        return HttpHookAsync(
            method=self.method,
            http_conn_id=self.http_conn_id,
        )
