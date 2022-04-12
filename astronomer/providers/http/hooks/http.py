import asyncio
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

import aiohttp
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from asgiref.sync import sync_to_async

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


class HttpHookAsync(BaseHook):
    """
    Interact with HTTP servers using Python Async.

    :param method: the API method to be called
    :param http_conn_id: http connection id that has the base
        API url i.e https://www.google.com/ and optional authentication credentials. Default
        headers can also be specified in the Extra field in json format.
    :param auth_type: The auth type for the service
    :type auth_type: AuthBase of python aiohttp lib
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "http_default"
    conn_type = "http"
    hook_name = "HTTP"

    def __init__(
        self,
        method: str = "POST",
        http_conn_id: str = default_conn_name,
        auth_type: Any = aiohttp.BasicAuth,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self.http_conn_id = http_conn_id
        self.method = method.upper()
        self.base_url: str = ""
        self._retry_obj: Callable[..., Any]
        self.auth_type: Any = auth_type
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    async def run(
        self,
        endpoint: Optional[str] = None,
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
    ) -> "ClientResponse":
        r"""
        Performs an asynchronous HTTP request call

        :param endpoint: the endpoint to be called i.e. resource/v1/query?
        :param data: payload to be uploaded or request parameters
        :param headers: additional headers to be passed through as a dictionary
        :param extra_options: Additional kwargs to pass when creating a request.
            For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
        """
        extra_options = extra_options or {}

        # headers may be passed through directly or in the "extra" field in the connection
        # definition
        _headers = {}
        auth = None

        if self.http_conn_id:
            conn = await sync_to_async(self.get_connection)(self.http_conn_id)

            if conn.host and "://" in conn.host:
                self.base_url = conn.host
            else:
                # schema defaults to HTTP
                schema = conn.schema if conn.schema else "http"
                host = conn.host if conn.host else ""
                self.base_url = schema + "://" + host

            if conn.port:
                self.base_url = self.base_url + ":" + str(conn.port)
            if conn.login:
                auth = self.auth_type(conn.login, conn.password)
            if conn.extra:
                try:
                    _headers.update(conn.extra_dejson)
                except TypeError:
                    self.log.warning("Connection to %s has invalid extra field.", conn.host)
        if headers:
            _headers.update(headers)

        if self.base_url and not self.base_url.endswith("/") and endpoint and not endpoint.startswith("/"):
            url = self.base_url + "/" + endpoint
        else:
            url = (self.base_url or "") + (endpoint or "")

        async with aiohttp.ClientSession() as session:
            if self.method == "GET":
                request_func = session.get
            elif self.method == "POST":
                request_func = session.post
            elif self.method == "PATCH":
                request_func = session.patch
            else:
                raise AirflowException(f"Unexpected HTTP Method: {self.method}")

            attempt_num = 1
            while True:
                response = await request_func(
                    url,
                    json=data if self.method in ("POST", "PATCH") else None,
                    params=data if self.method == "GET" else None,
                    headers=headers,
                    auth=auth,
                    **extra_options,
                )
                try:
                    response.raise_for_status()
                    return response
                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt_num,
                        self.retry_limit,
                        url,
                    )
                    if not self._retryable_error_async(e) or attempt_num == self.retry_limit:
                        self.log.exception("HTTP error with status: %s", e.status)
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        raise AirflowException(str(e.status) + ":" + e.message)

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def _retryable_error_async(self, exception: ClientResponseError) -> bool:
        """
        Determines whether or not an exception that was thrown might be successful
        on a subsequent attempt.

        It considers the following to be retryable:
            - requests_exceptions.ConnectionError
            - requests_exceptions.Timeout
            - anything with a status code >= 500

        Most retryable errors are covered by status code >= 500.
        """
        return exception.status >= 500
