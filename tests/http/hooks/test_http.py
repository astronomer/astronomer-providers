import logging
from unittest import mock

import pytest
from aiohttp.client_exceptions import ClientConnectionError
from airflow.exceptions import AirflowException
from airflow.models import Connection

from astronomer.providers.http.hooks.http import HttpHookAsync


class TestHttpHookAsync:
    @pytest.mark.asyncio
    async def test_do_api_call_async_non_retryable_error(self, aioresponse, monkeypatch):
        hook = HttpHookAsync(method="GET")
        aioresponse.get("http://httpbin.org/non_existent_endpoint", status=400)

        monkeypatch.setenv("AIRFLOW_CONN_HTTP_DEFAULT", "http://httpbin.org/")
        with pytest.raises(AirflowException) as exc:
            await hook.run(endpoint="non_existent_endpoint")

        assert str(exc.value) == "400:Bad Request"

    @pytest.mark.asyncio
    async def test_do_api_call_async_retryable_error(self, caplog, aioresponse, monkeypatch):
        caplog.set_level(logging.WARNING, logger="astronomer.providers.http.hooks.http")
        hook = HttpHookAsync(method="GET")
        aioresponse.get("http://httpbin.org/non_existent_endpoint", status=500, repeat=True)

        monkeypatch.setenv("AIRFLOW_CONN_HTTP_DEFAULT", "http://httpbin.org/")
        with pytest.raises(AirflowException) as exc:
            await hook.run(endpoint="non_existent_endpoint")

        assert str(exc.value) == "500:Internal Server Error"
        assert "[Try 3 of 3] Request to http://httpbin.org/non_existent_endpoint failed" in caplog.text

    @pytest.mark.asyncio
    async def test_do_api_call_async_unknown_method(self):
        hook = HttpHookAsync(method="NOPE")
        json = {
            "existing_cluster_id": "xxxx-xxxxxx-xxxxxx",
        }

        with pytest.raises(AirflowException) as exc:
            await hook.run(endpoint="non_existent_endpoint", data=json)

        assert str(exc.value) == "Unexpected HTTP Method: NOPE"

    @staticmethod
    def get_airflow_connection(unused_conn_id=None):
        return Connection(
            conn_id="http_default",
            conn_type="http",
            host="test",
            port=8080,
            extra='{"bearer": "test"}',
        )

    @pytest.mark.asyncio
    async def test_post_request(self, aioresponse):
        hook = HttpHookAsync()

        aioresponse.post(
            "http://test:8080/v1/test",
            status=200,
            payload='{"status":{"status": 200}}',
            reason="OK",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=self.get_airflow_connection,
        ):
            resp = await hook.run("v1/test")
            assert resp.status == 200

    @pytest.mark.asyncio
    async def test_post_request_and_get_json_without_keep_response(self, aioresponse):
        hook = HttpHookAsync()
        payload = '{"status":{"status": 200}}'

        aioresponse.post(
            "http://test:8080/v1/test",
            status=200,
            payload=payload,
            reason="OK",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=self.get_airflow_connection,
        ):
            resp = await hook.run("v1/test")
            with pytest.raises(ClientConnectionError, match="Connection closed"):
                await resp.json()

    @pytest.mark.asyncio
    async def test_post_request_and_get_json_with_keep_response(self, aioresponse):
        hook = HttpHookAsync(keep_response=True)
        payload = '{"status":{"status": 200}}'

        aioresponse.post(
            "http://test:8080/v1/test",
            status=200,
            payload=payload,
            reason="OK",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=self.get_airflow_connection,
        ):
            resp = await hook.run("v1/test")
            resp_payload = await resp.json()
            assert resp.status == 200
            assert resp_payload == payload

    @pytest.mark.asyncio
    async def test_post_request_with_error_code(self, aioresponse):
        hook = HttpHookAsync()

        aioresponse.post(
            "http://test:8080/v1/test",
            status=418,
            payload='{"status":{"status": 418}}',
            reason="I am teapot",
        )

        with mock.patch(
            "airflow.hooks.base.BaseHook.get_connection",
            side_effect=self.get_airflow_connection,
        ):
            with pytest.raises(AirflowException):
                await hook.run("v1/test")
