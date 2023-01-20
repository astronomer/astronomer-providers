"""This module contains the Apache Livy hook async."""
import asyncio
import re
from typing import Any, Dict, List, Optional, Sequence, Union

import aiohttp
from aiohttp import ClientResponseError
from airflow.models import Connection
from airflow.providers.apache.livy.hooks.livy import BatchState
from airflow.utils.log.logging_mixin import LoggingMixin
from asgiref.sync import sync_to_async

from astronomer.providers.http.hooks.http import HttpHookAsync


class LivyHookAsync(HttpHookAsync, LoggingMixin):
    """
    Hook for Apache Livy through the REST API using LivyHookAsync

    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :param extra_options: Additional option can be passed when creating a request.
            For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
    :param extra_headers: A dictionary of headers passed to the HTTP request to livy.

    .. seealso::
        For more details refer to the Apache Livy API reference:
        `Apache Livy API reference <https://livy.apache.org/docs/latest/rest-api.html>`_
    """

    TERMINAL_STATES = {
        BatchState.SUCCESS,
        BatchState.DEAD,
        BatchState.KILLED,
        BatchState.ERROR,
    }

    _def_headers = {"Content-Type": "application/json", "Accept": "application/json"}

    conn_name_attr = "livy_conn_id"
    default_conn_name = "livy_default"
    conn_type = "livy"
    hook_name = "Apache Livy"

    def __init__(
        self,
        livy_conn_id: str = default_conn_name,
        extra_options: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(http_conn_id=livy_conn_id)
        self.extra_headers = extra_headers or {}
        self.extra_options = extra_options or {}

    async def _do_api_call_async(
        self,
        endpoint: Optional[str] = None,
        data: Optional[Union[Dict[str, Any], str]] = None,
        headers: Optional[Dict[str, Any]] = None,
        extra_options: Optional[Dict[str, Any]] = None,
    ) -> Any:
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

            self.base_url = self._generate_base_url(conn)
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
                return {"Response": f"Unexpected HTTP Method: {self.method}", "status": "error"}

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
                    return await response.json()
                except ClientResponseError as e:
                    self.log.warning(
                        "[Try %d of %d] Request to %s failed.",
                        attempt_num,
                        self.retry_limit,
                        url,
                    )
                    if not self._retryable_error_async(e) or attempt_num == self.retry_limit:
                        self.log.exception("HTTP error, status code: %s", e.status)
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        return {"Response": {e.message}, "Status Code": {e.status}, "status": "error"}

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    def _generate_base_url(self, conn: Connection) -> str:
        if conn.host and "://" in conn.host:
            base_url: str = conn.host
        else:
            # schema defaults to HTTP
            schema = conn.schema if conn.schema else "http"
            host = conn.host if conn.host else ""
            base_url = f"{schema}://{host}"
        if conn.port:
            base_url = f"{base_url}:{conn.port}"
        return base_url

    async def run_method(
        self,
        endpoint: str,
        method: str = "GET",
        data: Optional[Any] = None,
        headers: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Wrapper for HttpHookAsync, allows to change method on the same HttpHookAsync

        :param method: http method
        :param endpoint: endpoint
        :param data: request payload
        :param headers: headers
        :return: http response
        :rtype: requests.Response
        """
        if method not in ("GET", "POST", "PUT", "DELETE", "HEAD"):
            return {"status": "error", "response": f"Invalid http method {method}"}

        back_method = self.method
        self.method = method
        try:
            result = await self._do_api_call_async(endpoint, data, headers, self.extra_options)
        finally:
            self.method = back_method
        return {"status": "success", "response": result}

    async def get_batch_state(self, session_id: Union[int, str]) -> Any:
        """
        Fetch the state of the specified batch asynchronously.

        :param session_id: identifier of the batch sessions
        :return: batch state
        :rtype: BatchState
        """
        self._validate_session_id(session_id)
        self.log.info("Fetching info for batch session %d", session_id)
        result = await self.run_method(endpoint=f"/batches/{session_id}/state")
        if result["status"] == "error":
            self.log.info(result)
            return {"batch_state": "error", "response": result, "status": "error"}

        if "state" not in result["response"]:
            self.log.info(
                "batch_state: error with as it is unable to get state for batch with id: %s", session_id
            )
            return {
                "batch_state": "error",
                "response": f"Unable to get state for batch with id: {session_id}",
                "status": "error",
            }

        self.log.info("Successfully fetched the batch state.")
        return {
            "batch_state": BatchState(result["response"]["state"]),
            "response": "successfully fetched the batch state.",
            "status": "success",
        }

    async def get_batch_logs(
        self, session_id: Union[int, str], log_start_position: int, log_batch_size: int
    ) -> Any:
        """
        Gets the session logs for a specified batch asynchronously.

        :param session_id: identifier of the batch sessions
        :param log_start_position: Position from where to pull the logs
        :param log_batch_size: Number of lines to pull in one batch

        :return: response body
        :rtype: dict
        """
        self._validate_session_id(session_id)
        log_params = {"from": log_start_position, "size": log_batch_size}
        result = await self.run_method(endpoint=f"/batches/{session_id}/log", data=log_params)
        if result["status"] == "error":
            self.log.info(result)
            return {"response": result["response"], "status": "error"}
        return {"response": result["response"], "status": "success"}

    async def dump_batch_logs(self, session_id: Union[int, str]) -> Any:
        """
        Dumps the session logs for a specified batch asynchronously

        :param session_id: identifier of the batch sessions
        :return: response body
        :rtype: dict
        """
        self.log.info("Fetching the logs for batch session with id: %d", session_id)
        log_start_line = 0
        log_total_lines = 0
        log_batch_size = 100

        while log_start_line <= log_total_lines:
            # Livy log endpoint is paginated.
            result = await self.get_batch_logs(session_id, log_start_line, log_batch_size)
            if result["status"] == "success":
                log_start_line += log_batch_size
                log_lines = self._parse_request_response(result["response"], "log")
                for log_line in log_lines:
                    self.log.info(log_line)
                return log_lines
            else:
                self.log.info(result["response"])
                return result["response"]

    @staticmethod
    def _validate_session_id(session_id: Union[int, str]) -> None:
        """
        Validate session id is a int

        :param session_id: session id
        """
        try:
            int(session_id)
        except (TypeError, ValueError):
            raise TypeError("'session_id' must be an integer")

    @staticmethod
    def _parse_post_response(response: Dict[Any, Any]) -> Any:
        """
        Parse batch response for batch id

        :param response: response body
        :return: session id
        :rtype: Any
        """
        return response.get("id")

    @staticmethod
    def _parse_request_response(response: Dict[Any, Any], parameter: Any) -> Any:
        """
        Parse batch response for batch id

        :param response: response body
        :return: value of parameter
        :rtype: Any
        """
        return response.get(parameter)

    @staticmethod
    def build_post_batch_body(
        file: str,
        args: Optional[Sequence[Union[str, int, float]]] = None,
        class_name: Optional[str] = None,
        jars: Optional[List[str]] = None,
        py_files: Optional[List[str]] = None,
        files: Optional[List[str]] = None,
        archives: Optional[List[str]] = None,
        name: Optional[str] = None,
        driver_memory: Optional[str] = None,
        driver_cores: Optional[Union[int, str]] = None,
        executor_memory: Optional[str] = None,
        executor_cores: Optional[int] = None,
        num_executors: Optional[Union[int, str]] = None,
        queue: Optional[str] = None,
        proxy_user: Optional[str] = None,
        conf: Optional[Dict[Any, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build the post batch request body.

        :param file: Path of the file containing the application to execute (required).
        :param proxy_user: User to impersonate when running the job.
        :param class_name: Application Java/Spark main class string.
        :param args: Command line arguments for the application s.
        :param jars: jars to be used in this sessions.
        :param py_files: Python files to be used in this session.
        :param files: files to be used in this session.
        :param driver_memory: Amount of memory to use for the driver process  string.
        :param driver_cores: Number of cores to use for the driver process int.
        :param executor_memory: Amount of memory to use per executor process  string.
        :param executor_cores: Number of cores to use for each executor  int.
        :param num_executors: Number of executors to launch for this session  int.
        :param archives: Archives to be used in this session.
        :param queue: The name of the YARN queue to which submitted string.
        :param name: The name of this session string.
        :param conf: Spark configuration properties.
        :return: request body
        :rtype: dict

        For more information about the format refer to
        .. seealso:: https://livy.apache.org/docs/latest/rest-api.html
        """
        body: Dict[str, Any] = {"file": file}

        if proxy_user:
            body["proxyUser"] = proxy_user
        if class_name:
            body["className"] = class_name
        if args and LivyHookAsync._validate_list_of_stringables(args):
            body["args"] = [str(val) for val in args]
        if jars and LivyHookAsync._validate_list_of_stringables(jars):
            body["jars"] = jars
        if py_files and LivyHookAsync._validate_list_of_stringables(py_files):
            body["pyFiles"] = py_files
        if files and LivyHookAsync._validate_list_of_stringables(files):
            body["files"] = files
        if driver_memory and LivyHookAsync._validate_size_format(driver_memory):
            body["driverMemory"] = driver_memory
        if driver_cores:
            body["driverCores"] = driver_cores
        if executor_memory and LivyHookAsync._validate_size_format(executor_memory):
            body["executorMemory"] = executor_memory
        if executor_cores:
            body["executorCores"] = executor_cores
        if num_executors:
            body["numExecutors"] = num_executors
        if archives and LivyHookAsync._validate_list_of_stringables(archives):
            body["archives"] = archives
        if queue:
            body["queue"] = queue
        if name:
            body["name"] = name
        if conf and LivyHookAsync._validate_extra_conf(conf):
            body["conf"] = conf

        return body

    @staticmethod
    def _validate_size_format(size: str) -> bool:
        """
        Validate size format.

        :param size: size value
        :return: true if valid format
        :rtype: bool
        """
        if size and not (isinstance(size, str) and re.match(r"^\d+[kmgt]b?$", size, re.IGNORECASE)):
            raise ValueError(f"Invalid java size format for string'{size}'")
        return True

    @staticmethod
    def _validate_list_of_stringables(vals: Sequence[Union[str, int, float]]) -> bool:
        """
        Check the values in the provided list can be converted to strings.

        :param vals: list to validate
        :return: true if valid
        :rtype: bool
        """
        if (
            vals is None
            or not isinstance(vals, (tuple, list))
            or any(1 for val in vals if not isinstance(val, (str, int, float)))
        ):
            raise ValueError("List of strings expected")
        return True

    @staticmethod
    def _validate_extra_conf(conf: Dict[Any, Any]) -> bool:
        """
        Check configuration values are either strings or ints.

        :param conf: configuration variable
        :return: true if valid
        :rtype: bool
        """
        if conf:
            if not isinstance(conf, dict):
                raise ValueError("'conf' argument must be a dict")
            if any(True for k, v in conf.items() if not (v and isinstance(v, str) or isinstance(v, int))):
                raise ValueError("'conf' values must be either strings or ints")
        return True
