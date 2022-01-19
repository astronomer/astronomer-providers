import logging
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.models import Connection

from astronomer_operators.http.hooks.http import HttpHookAsync


@pytest.mark.asyncio
async def test_do_api_call_async_non_retryable_error(aioresponse):
    hook = HttpHookAsync(method="GET")
    aioresponse.get("http://httpbin.org/non_existent_endpoint", status=400)

    with pytest.raises(AirflowException) as exc, mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_HTTP_DEFAULT="http://httpbin.org/",
    ):
        await hook.run(endpoint="non_existent_endpoint")

    assert str(exc.value) == "400:Bad Request"


@pytest.mark.asyncio
async def test_do_api_call_async_retryable_error(caplog, aioresponse):
    caplog.set_level(logging.WARNING, logger="astronomer_operators.http.hooks.http")
    hook = HttpHookAsync(method="GET")
    aioresponse.get("http://httpbin.org/non_existent_endpoint", status=500, repeat=True)

    with pytest.raises(AirflowException) as exc, mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_HTTP_DEFAULT="http://httpbin.org/",
    ):
        await hook.run(endpoint="non_existent_endpoint")

    assert str(exc.value) == "500:Internal Server Error"
    assert "[Try 3 of 3] Request to http://httpbin.org/non_existent_endpoint failed" in caplog.text


@pytest.mark.asyncio
async def test_do_api_call_async_unknown_method():
    hook = HttpHookAsync(method="NOPE")
    json = {
        "existing_cluster_id": "xxxx-xxxxxx-xxxxxx",
    }

    with pytest.raises(AirflowException) as exc:
        await hook.run(endpoint="non_existent_endpoint", data=json)

    assert str(exc.value) == "Unexpected HTTP Method: NOPE"


def get_airflow_connection(unused_conn_id=None):
    return Connection(
        conn_id="http_default",
        conn_type="http",
        host="test:8080/",
        extra='{"bearer": "test"}',
    )


@pytest.mark.asyncio
async def test_post_request(aioresponse):
    hook = HttpHookAsync()

    aioresponse.post(
        "http://test:8080/v1/test",
        status=200,
        payload='{"status":{"status": 200}}',
        reason="OK",
    )

    with mock.patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection):
        resp = await hook.run("v1/test")
        assert resp.status == 200


@pytest.mark.asyncio
async def test_post_request_with_error_code(aioresponse):
    hook = HttpHookAsync()

    aioresponse.post(
        "http://test:8080/v1/test",
        status=418,
        payload='{"status":{"status": 418}}',
        reason="I am teapot",
    )

    with mock.patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection):
        with pytest.raises(AirflowException):
            await hook.run("v1/test")
