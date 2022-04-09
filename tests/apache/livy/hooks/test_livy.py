import unittest
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


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
async def test_get_batch_state_running(mock_run_method):
    """
    Asserts the batch state as running with success response.
    """
    mock_run_method.return_value = {"status": "success", "response": {"state": BatchState.RUNNING}}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    state = await hook.get_batch_state(BATCH_ID)
    assert state == {
        "batch_state": BatchState.RUNNING,
        "response": "successfully fetched the batch state.",
        "status": "success",
    }


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
async def test_get_batch_state_error(mock_run_method):
    """
    Asserts the batch state as error with error response.
    """
    mock_run_method.return_value = {"status": "error", "response": {"state": "error"}}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    state = await hook.get_batch_state(BATCH_ID)
    assert state["status"] == "error"


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
async def test_get_batch_state_error_without_state(mock_run_method):
    """
    Asserts the batch state as error without state returned as part of mock.
    """
    mock_run_method.return_value = {"status": "success", "response": {}}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    state = await hook.get_batch_state(BATCH_ID)
    assert state["status"] == "error"


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
async def test_get_batch_logs_success(mock_run_method):
    """
    Asserts the batch log as success.
    """
    mock_run_method.return_value = {"status": "success", "response": {}}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    state = await hook.get_batch_logs(BATCH_ID, 0, 100)
    assert state["status"] == "success"


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.run_method")
async def test_get_batch_logs_error(mock_run_method):
    """
    Asserts the batch log for error.
    """
    mock_run_method.return_value = {"status": "error", "response": {}}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    state = await hook.get_batch_logs(BATCH_ID, 0, 100)
    assert state["status"] == "error"


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_logs")
async def test_dump_batch_logs_success(mock_get_batch_logs):
    """
    Asserts the log dump log for success response.
    """
    mock_get_batch_logs.return_value = {
        "status": "success",
        "response": {"id": 1, "log": ["mock_log_1", "mock_log_2", "mock_log_3"]},
    }
    hook = LivyHookAsync(livy_conn_id="livy_default")
    log_dump = await hook.dump_batch_logs(BATCH_ID)
    assert log_dump == ["mock_log_1", "mock_log_2", "mock_log_3"]


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_batch_logs")
async def test_dump_batch_logs_error(mock_get_batch_logs):
    """
    Asserts the log dump log for error response.
    """
    mock_get_batch_logs.return_value = {
        "status": "error",
        "response": {"id": 1, "log": ["mock_log_1", "mock_log_2"]},
    }
    hook = LivyHookAsync(livy_conn_id="livy_default")
    log_dump = await hook.dump_batch_logs(BATCH_ID)
    assert log_dump == {"id": 1, "log": ["mock_log_1", "mock_log_2"]}


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync._do_api_call_async")
async def test_run_method_success(mock_do_api_call_async):
    """
    Asserts the run_method for success response.
    """
    mock_do_api_call_async.return_value = {"status": "error", "response": {"id": 1}}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    response = await hook.run_method("localhost", "GET")
    assert response["status"] == "success"


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync._do_api_call_async")
async def test_run_method_error(mock_do_api_call_async):
    """
    Asserts the run_method for error response.
    """
    mock_do_api_call_async.return_value = {"status": "error", "response": {"id": 1}}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    response = await hook.run_method("localhost", "abc")
    assert response == {"status": "error", "response": "Invalid http method abc"}


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
async def test_do_api_call_async_post_method_with_success(mock_get_connection, mock_session):
    """
    Asserts the _do_api_call_async for success response for POST method.
    """

    async def mock_fun(arg1, arg2, arg3, arg4):
        return {"status": "success"}

    mock_session.return_value.__aexit__.return_value = mock_fun
    mock_session.return_value.__aenter__.return_value.post.return_value.json.return_value = {
        "status": "success"
    }
    GET_RUN_ENDPOINT = "api/jobs/runs/get"
    hook = LivyHookAsync(livy_conn_id="livy_default")
    hook.http_conn_id = mock_get_connection
    hook.http_conn_id.host = "https://localhost"
    hook.http_conn_id.login = "login"
    hook.http_conn_id.password = "PASSWORD"
    response = await hook._do_api_call_async(GET_RUN_ENDPOINT)
    assert response == {"status": "success"}


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
async def test_do_api_call_async_get_method_with_success(mock_get_connection, mock_session):
    """
    Asserts the _do_api_call_async for GET method.
    """

    async def mock_fun(arg1, arg2, arg3, arg4):
        return {"status": "success"}

    mock_session.return_value.__aexit__.return_value = mock_fun
    mock_session.return_value.__aenter__.return_value.get.return_value.json.return_value = {
        "status": "success"
    }
    GET_RUN_ENDPOINT = "api/jobs/runs/get"
    hook = LivyHookAsync(livy_conn_id="livy_default")
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
async def test_do_api_call_async_patch_method_with_success(mock_get_connection, mock_session):
    """
    Asserts the _do_api_call_async for PATCH method.
    """

    async def mock_fun(arg1, arg2, arg3, arg4):
        return {"status": "success"}

    mock_session.return_value.__aexit__.return_value = mock_fun
    mock_session.return_value.__aenter__.return_value.patch.return_value.json.return_value = {
        "status": "success"
    }
    GET_RUN_ENDPOINT = "api/jobs/runs/get"
    hook = LivyHookAsync(livy_conn_id="livy_default")
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
async def test_do_api_call_async_unexpected_method_error(mock_get_connection, mock_session):
    """
    Asserts the _do_api_call_async for unexpected method error
    """
    GET_RUN_ENDPOINT = "api/jobs/runs/get"
    hook = LivyHookAsync(livy_conn_id="livy_default")
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
async def test_do_api_call_async_with_type_error(mock_get_connection, mock_session):
    """
    Asserts the _do_api_call_async for TypeError.
    """

    async def mock_fun(arg1, arg2, arg3, arg4):
        return {"random value"}

    mock_session.return_value.__aexit__.return_value = mock_fun
    mock_session.return_value.__aenter__.return_value.patch.return_value.json.return_value = {}
    hook = LivyHookAsync(livy_conn_id="livy_default")
    hook.method = "PATCH"
    hook.retry_limit = 1
    hook.retry_delay = 1
    hook.http_conn_id = mock_get_connection
    with pytest.raises(TypeError):
        await hook._do_api_call_async(endpoint="", data="test", headers=mock_fun, extra_options=mock_fun)


@pytest.mark.asyncio
@mock.patch("astronomer.providers.apache.livy.hooks.livy.aiohttp.ClientSession")
@mock.patch("astronomer.providers.apache.livy.hooks.livy.LivyHookAsync.get_connection")
async def test_do_api_call_async_with_client_response_error(mock_get_connection, mock_session):
    """
    Asserts the _do_api_call_async for Client Response Error.
    """

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


class TestLivyHookAsync(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        db.merge_conn(
            Connection(conn_id="livy_default", conn_type="http", host="host", schema="http", port=8998)
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

        connection_url_mapping = {
            # id, expected
            "default_port": "http://host",
            "default_protocol": "http://host",
            "port_set": "http://host:1234",
            "schema_set": "zzz://host",
            "dont_override_schema": "http://host",
        }

        for conn_id, expected in connection_url_mapping.items():
            with self.subTest(conn_id):
                hook = LivyHookAsync(livy_conn_id=conn_id)
                response_conn: Connection = hook.get_connection(conn_id=conn_id)
                assert isinstance(response_conn, Connection)
                assert hook._generate_base_url(response_conn) == expected

    def test_build_body(self):
        with self.subTest("minimal request"):
            body = LivyHookAsync.build_post_batch_body(file="appname")

            assert body == {"file": "appname"}

        with self.subTest("complex request"):
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
        with self.subTest("not a size"):
            with pytest.raises(ValueError):
                LivyHookAsync.build_post_batch_body(file="appname", executor_memory="xxx")

        with self.subTest("list of stringables"):
            assert LivyHookAsync.build_post_batch_body(file="appname", args=["a", 1, 0.1])["args"] == [
                "a",
                "1",
                "0.1",
            ]

    def test_parse_post_response(self):
        res_id = LivyHookAsync._parse_post_response({"id": BATCH_ID, "log": []})

        assert BATCH_ID == res_id

    def test_validate_size_format(self):
        with self.subTest("lower 1"):
            assert LivyHookAsync._validate_size_format("1m")

        with self.subTest("lower 2"):
            assert LivyHookAsync._validate_size_format("1mb")

        with self.subTest("upper 1"):
            assert LivyHookAsync._validate_size_format("1G")

        with self.subTest("upper 2"):
            assert LivyHookAsync._validate_size_format("1GB")

        with self.subTest("snake 1"):
            assert LivyHookAsync._validate_size_format("1Gb")

        with self.subTest("fullmatch"):
            with pytest.raises(ValueError):
                assert LivyHookAsync._validate_size_format("1Gb foo")

        with self.subTest("missing size"):
            with pytest.raises(ValueError):
                assert LivyHookAsync._validate_size_format("10")

        with self.subTest("numeric"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_size_format(1)

        with self.subTest("None"):
            assert LivyHookAsync._validate_size_format(None)

    def test_validate_list_of_stringables(self):
        with self.subTest("valid list"):
            try:
                LivyHookAsync._validate_list_of_stringables([1, "string"])
            except ValueError:
                self.fail("Exception raised")

        with self.subTest("valid tuple"):
            try:
                LivyHookAsync._validate_list_of_stringables((1, "string"))
            except ValueError:
                self.fail("Exception raised")

        with self.subTest("empty list"):
            try:
                LivyHookAsync._validate_list_of_stringables([])
            except ValueError:
                self.fail("Exception raised")

        with self.subTest("dict"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_list_of_stringables({"a": "a"})

        with self.subTest("invalid element"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_list_of_stringables([1, {}])

        with self.subTest("dict"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_list_of_stringables([1, None])

        with self.subTest("None"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_list_of_stringables(None)

        with self.subTest("int"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_list_of_stringables(1)

        with self.subTest("string"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_list_of_stringables("string")

    def test_validate_extra_conf(self):
        with self.subTest("valid"):
            try:
                LivyHookAsync._validate_extra_conf({"k1": "v1", "k2": 0})
            except ValueError:
                self.fail("Exception raised")

        with self.subTest("empty dict"):
            try:
                LivyHookAsync._validate_extra_conf({})
            except ValueError:
                self.fail("Exception raised")

        with self.subTest("none"):
            try:
                LivyHookAsync._validate_extra_conf(None)
            except ValueError:
                self.fail("Exception raised")

        with self.subTest("not a dict 1"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_extra_conf("k1=v1")

        with self.subTest("not a dict 2"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_extra_conf([("k1", "v1"), ("k2", 0)])

        with self.subTest("nested dict"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_extra_conf({"outer": {"inner": "val"}})

        with self.subTest("empty items"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_extra_conf({"has_val": "val", "no_val": None})

        with self.subTest("empty string"):
            with pytest.raises(ValueError):
                LivyHookAsync._validate_extra_conf({"has_val": "val", "no_val": ""})

    def test_parse_request_response(self):
        res_id = LivyHookAsync._parse_request_response(response={"id": BATCH_ID, "log": []}, parameter="id")

        assert BATCH_ID == res_id

    def test_check_session_id(self):
        with self.subTest("valid 00"):
            try:
                LivyHookAsync._validate_session_id(100)
            except TypeError:
                self.fail("")

        with self.subTest("valid 01"):
            try:
                LivyHookAsync._validate_session_id(0)
            except TypeError:
                self.fail("")

        with self.subTest("None"):
            with pytest.raises(TypeError):
                LivyHookAsync._validate_session_id(None)

        with self.subTest("random string"):
            with pytest.raises(TypeError):
                LivyHookAsync._validate_session_id("asd")
