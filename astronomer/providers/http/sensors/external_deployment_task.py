from typing import Any, Dict, Optional

from astronomer.providers.http.sensors.http import HttpSensorAsync
from astronomer.providers.http.triggers.http import ExternalDeploymentTaskTrigger
from astronomer.providers.utils.typing_compat import Context


class ExternalDeploymentTaskSensorAsync(HttpSensorAsync):
    """
    External deployment task sensor check for the response state success, continue polling till
    the response state is success. Inherits from HttpSensorAsync which

    :param http_conn_id: The Connection ID to run the sensor against
    :param method: The HTTP request method to use
    :param endpoint: The relative part of the full url
    :param request_params: The parameters to be added to the GET url
    :param headers: The HTTP headers to be added to the GET request
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    """

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=self.execution_timeout,
            trigger=ExternalDeploymentTaskTrigger(
                method=self.hook.method,
                endpoint=self.endpoint,
                data=self.request_params,
                headers=self.headers,
                extra_options=self.extra_options,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Optional[Dict[Any, Any]] = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Return true and log the response if state is not success state raise ValueError
        """
        if event.get("state"):
            if event["state"] == "success":
                self.log.info("Task Succeeded with response: %s", event)
                return True
        raise ValueError(f"Task Failed with response: {event}")
