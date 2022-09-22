from typing import Any, Dict, Optional

from astronomer.providers.http.sensors.http import HttpSensorAsync
from astronomer.providers.http.triggers.http import ExternalDeploymentTaskTrigger
from astronomer.providers.utils.typing_compat import Context


class ExternalDeploymentTaskSensorAsync(HttpSensorAsync):
    """
    External deployment task sensor Make API call and poll for the response state of the deployment.
    Inherits from HttpSensorAsync which

    :param http_conn_id: The Connection ID to run the sensor against
    :param method: The HTTP request method to use
    :param endpoint: The relative part of the full url
    :param request_params: The parameters to be added to the GET url
    :param headers: The HTTP headers to be added to the GET request
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    :param poke_interval: Time in seconds that the job should wait in between each tries
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
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event: Optional[Dict[str, Any]] = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Return true and log the response if state is not success state raise ValueError
        """
        if event and "state" in event:
            if event["state"] == "success":
                self.log.info("Task Succeeded with response: %s", event)
                return True
        raise ValueError(f"Task Failed with response: {event}")
