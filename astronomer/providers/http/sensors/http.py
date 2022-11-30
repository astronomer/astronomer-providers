import warnings
from datetime import timedelta
from typing import Any, Dict, Optional

from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.sensors.http import HttpSensor

from astronomer.providers.http.triggers.http import HttpTrigger
from astronomer.providers.utils.typing_compat import Context


class HttpSensorAsync(HttpSensor):
    """
    Executes a HTTP GET statement and returns False on failure caused by
    404 Not Found or `response_check` returning False.

    .. note::
        If ``response_check`` is passed, the sync version of the sensor will be used.

    The response check can access the template context to the operator:

    .. code-block:: python

        def response_check(response, task_instance):
            # The task_instance is injected, so you can pull data form xcom
            # Other context variables such as dag, ds, execution_date are also available.
            xcom_data = task_instance.xcom_pull(task_ids="pushing_task")
            # In practice you would do something more sensible with this data..
            print(xcom_data)
            return True


        HttpSensorAsync(task_id="my_http_sensor", ..., response_check=response_check)

    :param http_conn_id: The Connection ID to run the sensor against
    :type http_conn_id: str
    :param method: The HTTP request method to use
    :type method: str
    :param endpoint: The relative part of the full url
    :type endpoint: str
    :param request_params: The parameters to be added to the GET url
    :type request_params: a dictionary of string key/value pairs
    :param headers: The HTTP headers to be added to the GET request
    :type headers: a dictionary of string key/value pairs
    :param response_check: A check against the 'requests' response object.
        The callable takes the response object as the first positional argument
        and optionally any number of keyword arguments available in the context dictionary.
        It should return True for 'pass' and False otherwise.
        Currently if this parameter is specified then sync version of the sensor will be used.
    :type response_check: A lambda or defined function.
    :param extra_options: Extra options for the 'requests' library, see the
        'requests' documentation (options to modify timeout, ssl, etc.)
    :type extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    :param tcp_keep_alive: Enable TCP Keep Alive for the connection.
    :param tcp_keep_alive_idle: The TCP Keep Alive Idle parameter (corresponds to ``socket.TCP_KEEPIDLE``).
    :param tcp_keep_alive_count: The TCP Keep Alive count parameter (corresponds to ``socket.TCP_KEEPCNT``)
    :param tcp_keep_alive_interval: The TCP Keep Alive interval parameter (corresponds to
        ``socket.TCP_KEEPINTVL``)
    """

    def __init__(
        self,
        *,
        endpoint: str,
        poll_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        self.endpoint = endpoint
        # TODO: Remove once deprecated
        if poll_interval:
            self.poke_interval = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(endpoint=endpoint, **kwargs)
        try:
            # for apache-airflow-providers-http>=4.0.0
            self.hook = HttpHook(
                method=self.method,
                http_conn_id=self.http_conn_id,
                tcp_keep_alive=self.tcp_keep_alive,
                tcp_keep_alive_idle=self.tcp_keep_alive_idle,
                tcp_keep_alive_count=self.tcp_keep_alive_count,
                tcp_keep_alive_interval=self.tcp_keep_alive_interval,
            )
        except AttributeError:
            # for apache-airflow-providers-http<4.0.0
            # Since the hook is an instance variable of the operator, we need no action.
            pass

    def execute(self, context: Context) -> None:
        """
        Logic that the sensor uses to correctly identify which trigger to
        execute, and defer execution as expected.
        """
        # TODO: We can't currently serialize arbitrary function
        # Maybe we set method_name as users function??? to run it again
        # and evaluate the response.
        if self.response_check:
            self.log.warning("Since response_check param is passed, using the sync version of the sensor.")
            super().execute(context=context)
        else:
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=HttpTrigger(
                    method=self.hook.method,  # TODO: Fix this to directly get method from ctor
                    endpoint=self.endpoint,
                    data=self.request_params,
                    headers=self.headers,
                    extra_options=self.extra_options,
                    poke_interval=self.poke_interval,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: Optional[Dict[Any, Any]] = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None
