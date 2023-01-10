from unittest import mock

import multidict
import pytest
from aiohttp import ClientResponseError, RequestInfo
from airflow.models import Connection
from airflow.providers.apache.livy.hooks.livy import BatchState
from airflow.utils import db

from astronomer.providers.apache.livy.hooks.livy import LivyHookAsync

BATCH_ID = 100
SAMPLE_GET_RESPONSE = {"id": BATCH_ID, "state": BatchState.SUCCESS.value}
LIVY_CONN_ID = "livy_default"


class TestLivyHookAsync:
    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
    async def test_get_batch_state_running(self, mock_run_method):
        """Asserts the batch state as running with success response."""
        mock_run_method.return_value = {"status": "success", "response": {"state": BatchState.RUNNING}}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_state(BATCH_ID)
        assert state == {
            "batch_state": BatchState.RUNNING,
            "response": "successfully fetched the batch state.",
            "status": "success",
        }

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
    async def test_get_batch_state_error(self, mock_run_method):
        """Asserts the batch state as error with error response."""
        mock_run_method.return_value = {"status": "error", "response": {"state": "error"}}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_state(BATCH_ID)
        assert state["status"] == "error"

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
    async def test_get_batch_state_error_without_state(self, mock_run_method):
        """Asserts the batch state as error without state returned as part of mock."""
        mock_run_method.return_value = {"status": "success", "response": {}}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_state(BATCH_ID)
        assert state["status"] == "error"

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
    async def test_get_batch_logs_success(self, mock_run_method):
        """Asserts the batch log as success."""
        mock_run_method.return_value = {"status": "success", "response": {}}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_logs(BATCH_ID, 0, 100)
        assert state["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
    async def test_get_batch_logs_error(self, mock_run_method):
        """Asserts the batch log for error."""
        mock_run_method.return_value = {"status": "error", "response": {}}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        state = await hook.get_batch_logs(BATCH_ID, 0, 100)
        assert state["status"] == "error"

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_logs")
    async def test_dump_batch_logs_success(self, mock_get_batch_logs):
        """Asserts the log dump log for success response."""
        mock_get_batch_logs.return_value = {
            "status": "success",
            "response": {"id": 1, "log": ["mock_log_1", "mock_log_2", "mock_log_3"]},
        }
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        log_dump = await hook.dump_batch_logs(BATCH_ID)
        assert log_dump == ["mock_log_1", "mock_log_2", "mock_log_3"]

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_logs")
    async def test_dump_batch_logs_error(self, mock_get_batch_logs):
        """Asserts the log dump log for error response."""
        mock_get_batch_logs.return_value = {
            "status": "error",
            "response": {"id": 1, "log": ["mock_log_1", "mock_log_2"]},
        }
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        log_dump = await hook.dump_batch_logs(BATCH_ID)
        assert log_dump == {"id": 1, "log": ["mock_log_1", "mock_log_2"]}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync._do_api_call_async")
    async def test_run_method_success(self, mock_do_api_call_async):
        """Asserts the run_method for success response."""
        mock_do_api_call_async.return_value = {"status": "error", "response": {"id": 1}}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        response = await hook.run_method("localhost", "GET")
        assert response["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync._do_api_call_async")
    async def test_run_method_error(self, mock_do_api_call_async):
        """Asserts the run_method for error response."""
        mock_do_api_call_async.return_value = {"status": "error", "response": {"id": 1}}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        response = await hook.run_method("localhost", "abc")
        assert response == {"status": "error", "response": "Invalid http method abc"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
    async def test_do_api_call_async_post_method_with_success(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for success response for POST method."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.post.return_value.json.return_value = {
            "status": "success"
        }
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "https://localhost"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response == {"status": "success"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
    async def test_do_api_call_async_get_method_with_success(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for GET method."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.get.return_value.json.return_value = {
            "status": "success"
        }
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        hook.method = "GET"
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response == {"status": "success"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
    async def test_do_api_call_async_patch_method_with_success(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for PATCH method."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.patch.return_value.json.return_value = {
            "status": "success"
        }
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        hook.method = "PATCH"
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response == {"status": "success"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
    async def test_do_api_call_async_unexpected_method_error(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for unexpected method error"""
        GET_RUN_ENDPOINT = "api/jobs/runs/get"
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        hook.method = "abc"
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(endpoint=GET_RUN_ENDPOINT, headers={})
        assert response == {"Response": "Unexpected HTTP Method: abc", "status": "error"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
    async def test_do_api_call_async_with_type_error(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for TypeError."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"random value"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.patch.return_value.json.return_value = {}
        hook = LivyHookAsync(livy_conn_id=LIVY_CONN_ID)
        hook.method = "PATCH"
        hook.retry_limit = 1
        hook.retry_delay = 1
        hook.http_conn_id = mock_get_connection
        with pytest.raises(TypeError):
            await hook._do_api_call_async(endpoint="", data="test", headers=mock_fun, extra_options=mock_fun)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
    @mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
    async def test_do_api_call_async_with_client_response_error(self, mock_get_connection, mock_session):
        """Asserts the _do_api_call_async for Client Response Error."""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"random value"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.patch.return_value.json.side_effect = (
            ClientResponseError(
                request_info=RequestInfo(url="example.com", method="PATCH", headers=multidict.CIMultiDict()),
                status=500,
                history=[],
            )
        )
        GET_RUN_ENDPOINT = ""
        hook = LivyHookAsync(livy_conn_id="livy_default")
        hook.method = "PATCH"
        hook.base_url = ""
        hook.http_conn_id = mock_get_connection
        hook.http_conn_id.host = "test.com"
        hook.http_conn_id.login = "login"
        hook.http_conn_id.password = "PASSWORD"
        hook.http_conn_id.extra_dejson = ""
        response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
        assert response["status"] == "error"

    def set_conn(self):
        db.merge_conn(
            Connection(conn_id=LIVY_CONN_ID, conn_type="http", host="host", schema="http", port=8998)
        )
        db.merge_conn(Connection(conn_id="default_port", conn_type="http", host="http://host"))
        db.merge_conn(Connection(conn_id="default_protocol", conn_type="http", host="host"))
        db.merge_conn(Connection(conn_id="port_set", host="host", conn_type="http", port=1234))
        db.merge_conn(Connection(conn_id="schema_set", host="host", conn_type="http", schema="zzz"))
        db.merge_conn(
            Connection(conn_id="dont_override_schema", conn_type="http", host="http://host", schema="zzz")
        )
        db.merge_conn(Connection(conn_id="missing_host", conn_type="http", port=1234))
        db.merge_conn(Connection(conn_id="invalid_uri", uri="http://invalid_uri:4321"))

    def test_build_get_hook(self):
        self.set_conn()
        connection_url_mapping = {
            # id, expected
            "default_port": "http://host",
            "default_protocol": "http://host",
            "port_set": "http://host:1234",
            "schema_set": "zzz://host",
            "dont_override_schema": "http://host",
        }

        for conn_id, expected in connection_url_mapping.items():
            hook = LivyHookAsync(livy_conn_id=conn_id)
            response_conn: Connection = hook.get_connection(conn_id=conn_id)
            assert isinstance(response_conn, Connection)
            assert hook._generate_base_url(response_conn) == expected

    def test_build_body(self):
        # minimal request
        body = LivyHookAsync.build_post_batch_body(file="appname")

        assert body == {"file": "appname"}

        # complex request
        body = LivyHookAsync.build_post_batch_body(
            file="appname",
            class_name="org.example.livy",
            proxy_user="proxyUser",
            args=["a", "1"],
            jars=["jar1", "jar2"],
            files=["file1", "file2"],
            py_files=["py1", "py2"],
            archives=["arch1", "arch2"],
            queue="queue",
            name="name",
            conf={"a": "b"},
            driver_cores=2,
            driver_memory="1M",
            executor_memory="1m",
            executor_cores="1",
            num_executors="10",
        )

        assert body == {
            "file": "appname",
            "className": "org.example.livy",
            "proxyUser": "proxyUser",
            "args": ["a", "1"],
            "jars": ["jar1", "jar2"],
            "files": ["file1", "file2"],
            "pyFiles": ["py1", "py2"],
            "archives": ["arch1", "arch2"],
            "queue": "queue",
            "name": "name",
            "conf": {"a": "b"},
            "driverCores": 2,
            "driverMemory": "1M",
            "executorMemory": "1m",
            "executorCores": "1",
            "numExecutors": "10",
        }

    def test_parameters_validation(self):
        with pytest.raises(ValueError):
            LivyHookAsync.build_post_batch_body(file="appname", executor_memory="xxx")

        assert LivyHookAsync.build_post_batch_body(file="appname", args=["a", 1, 0.1])["args"] == [
            "a",
            "1",
            "0.1",
        ]

    def test_parse_post_response(self):
        res_id = LivyHookAsync._parse_post_response({"id": BATCH_ID, "log": []})

        assert BATCH_ID == res_id

    @pytest.mark.parametrize("valid_size", ["1m", "1mb", "1G", "1GB", "1Gb", None])
    def test_validate_size_format_success(self, valid_size):
        assert LivyHookAsync._validate_size_format(valid_size)

    @pytest.mark.parametrize("invalid_size", ["1Gb foo", "10", 1])
    def test_validate_size_format_failure(self, invalid_size):
        with pytest.raises(ValueError):
            assert LivyHookAsync._validate_size_format(invalid_size)

    @pytest.mark.parametrize(
        "valid_string",
        [
            [1, "string"],
            (1, "string"),
            [],
        ],
    )
    def test_validate_list_of_stringables_success(self, valid_string):
        assert LivyHookAsync._validate_list_of_stringables(valid_string)

    @pytest.mark.parametrize("invalid_string", [{"a": "a"}, [1, {}], [1, None], None, 1, "string"])
    def test_validate_list_of_stringables_failure(self, invalid_string):
        with pytest.raises(ValueError):
            LivyHookAsync._validate_list_of_stringables(invalid_string)

    @pytest.mark.parametrize(
        "conf",
        [
            {"k1": "v1", "k2": 0},
            {},
            None,
        ],
    )
    def test_validate_extra_conf_success(self, conf):
        assert LivyHookAsync._validate_extra_conf(conf)

    @pytest.mark.parametrize(
        "conf",
        [
            "k1=v1",
            [("k1", "v1"), ("k2", 0)],
            {"outer": {"inner": "val"}},
            {"has_val": "val", "no_val": None},
            {"has_val": "val", "no_val": ""},
        ],
    )
    def test_validate_extra_conf_failure(self, conf):
        with pytest.raises(ValueError):
            LivyHookAsync._validate_extra_conf(conf)

    def test_parse_request_response(self):
        assert BATCH_ID == LivyHookAsync._parse_request_response(
            response={"id": BATCH_ID, "log": []}, parameter="id"
        )

    @pytest.mark.parametrize("conn_id", [100, 0])
    def test_check_session_id_success(self, conn_id):
        assert LivyHookAsync._validate_session_id(conn_id) is None

    @pytest.mark.parametrize("conn_id", [None, "asd"])
    def test_check_session_id_failure(self, conn_id):
        with pytest.raises(TypeError):
            LivyHookAsync._validate_session_id(None)
