import json
import logging
from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import (
    GET_RUN_ENDPOINT,
    SUBMIT_RUN_ENDPOINT,
)
from asgiref.sync import sync_to_async

from astronomer_operators.hooks.databricks import DatabricksHookAsync

TASK_ID = "databricks_check"
CONN_ID = "unit_test_conn_id"
RUN_ID = "unit_test_run_id"
LOGIN = "login"
PASSWORD = "password"
TOKEN = "token"


@pytest.mark.asyncio
@mock.patch(
    "astronomer_operators.hooks.databricks.DatabricksHookAsync._do_api_call_async"
)
async def test_databricks_hook_get_run_state(mocked_response):
    """
    Asserts that a run state is returned as expected while a Databricks run
    is in a PENDING state (i.e. "RUNNING") and after it reaches a TERMINATED
    state (i.e. "SUCCESS").
    """
    hook = DatabricksHookAsync()
    # Mock response while job is running
    mocked_response.return_value = {
        "state": {
            "life_cycle_state": "RUNNING",
            "result_state": "",
            "state_message": "In run",
            "user_cancelled_or_timedout": "False",
        }
    }
    run_state_running = await hook.get_run_state_async(RUN_ID)

    assert run_state_running.life_cycle_state == "RUNNING"
    assert run_state_running.result_state == ""
    assert run_state_running.state_message == "In run"

    # Mock response after job is complete
    mocked_response.return_value = {
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "SUCCESS",
            "state_message": "",
            "user_cancelled_or_timedout": "False",
        }
    }
    run_state_complete = await hook.get_run_state_async(RUN_ID)

    assert run_state_complete.life_cycle_state == "TERMINATED"
    assert run_state_complete.result_state == "SUCCESS"
    assert run_state_complete.state_message == ""


@pytest.mark.asyncio
async def test_do_api_call_async_get_basic_auth(caplog, aioresponse):
    """
    Asserts that the Databricks hook makes a GET call as expected when
    provided with basic auth credentials.
    The aiohttp module assumes basic auth for its 'auth' parameter, so
    we need to set this manually in the header for both bearer token
    and basic auth.
    """
    caplog.set_level(logging.INFO)
    hook = DatabricksHookAsync()
    hook.databricks_conn = await sync_to_async(hook.get_connection)(
        hook.databricks_conn_id
    )
    hook.databricks_conn.login = LOGIN
    hook.databricks_conn.password = PASSWORD
    params = {"run_id": RUN_ID}

    aioresponse.get(
        "https://localhost/api/2.0/jobs/runs/get?run_id=unit_test_run_id",
        status=200,
        body='{"result":"Yay!"}',
    )
    resp = await hook._do_api_call_async(GET_RUN_ENDPOINT, params)
    assert resp == {"result": "Yay!"}
    assert resp["result"] == "Yay!"
    assert "Using basic auth. " in caplog.text


@pytest.mark.asyncio
async def test_do_api_call_async_get_auth_token(caplog, aioresponse):
    """
    Asserts that the Databricks hook makes a GET call as expected when
    provided with an auth token.
    The aiohttp module assumes basic auth for its 'auth' parameter, so
    we need to set this manually in the header for both bearer token
    and basic auth.
    """
    caplog.set_level(logging.INFO)
    hook = DatabricksHookAsync()
    hook.databricks_conn = await sync_to_async(hook.get_connection)(
        hook.databricks_conn_id
    )
    hook.databricks_conn.extra = json.dumps({"token": "test_token"})
    params = {"run_id": RUN_ID}

    aioresponse.get(
        "https://localhost/api/2.0/jobs/runs/get?run_id=unit_test_run_id",
        status=200,
        body='{"result":"Yay!"}',
    )
    resp = await hook._do_api_call_async(GET_RUN_ENDPOINT, params)
    assert resp == {"result": "Yay!"}
    assert resp["result"] == "Yay!"
    assert "Using token auth. " in caplog.text


@pytest.mark.asyncio
async def test_do_api_call_async_non_retryable_error(aioresponse):
    """
    Asserts that the Databricks hook will throw an exception
    when a non-retryable error is returned by the API.
    """
    hook = DatabricksHookAsync()
    hook.databricks_conn = await sync_to_async(hook.get_connection)(
        hook.databricks_conn_id
    )
    hook.databricks_conn.login = LOGIN
    hook.databricks_conn.password = PASSWORD
    params = {"run_id": RUN_ID}

    aioresponse.get(
        "https://localhost/api/2.0/jobs/runs/get?run_id=unit_test_run_id",
        status=400,
    )

    with pytest.raises(AirflowException) as exc:
        await hook._do_api_call_async(GET_RUN_ENDPOINT, params)

    assert str(exc.value) == "Response: Bad Request, Status Code: 400"


@pytest.mark.asyncio
async def test_do_api_call_async_retryable_error(aioresponse):
    """
    Asserts that the Databricks hook will attempt another API call as many
    times as the retry_limit when a retryable error is returned by the API.
    """
    hook = DatabricksHookAsync()
    hook.databricks_conn = await sync_to_async(hook.get_connection)(
        hook.databricks_conn_id
    )
    hook.databricks_conn.login = LOGIN
    hook.databricks_conn.password = PASSWORD
    params = {"run_id": RUN_ID}

    aioresponse.get(
        "https://localhost/api/2.0/jobs/runs/get?run_id=unit_test_run_id",
        status=500,
        repeat=True,
    )

    with pytest.raises(AirflowException) as exc:
        await hook._do_api_call_async(GET_RUN_ENDPOINT, params)

    assert (
        str(exc.value)
        == f"API requests to Databricks failed {hook.retry_limit} times. Giving up."
    )


@pytest.mark.asyncio
async def test_do_api_call_async_post(aioresponse):
    """
    Asserts that the Databricks hook makes a POST call as expected.
    """
    hook = DatabricksHookAsync()
    hook.databricks_conn = await sync_to_async(hook.get_connection)(
        hook.databricks_conn_id
    )
    hook.databricks_conn.login = LOGIN
    hook.databricks_conn.password = PASSWORD
    json = {
        "task_id": TASK_ID,
        "existing_cluster_id": "xxxx-xxxxxx-xxxxxx",
        "notebook_task": {"notebook_path": "/Users/test@astronomer.io/test_notebook"},
    }

    aioresponse.post(
        "https://localhost/api/2.0/jobs/runs/submit",
        status=202,
        body='{"result":"Yay!"}',
    )
    resp = await hook._do_api_call_async(SUBMIT_RUN_ENDPOINT, json)
    assert resp == {"result": "Yay!"}
    assert resp["result"] == "Yay!"


@pytest.mark.asyncio
async def test_do_api_call_async_unknown_method():
    """
    Asserts that the Databricks hook throws an exception when it attempts to
    make an API call using a non-existent method.
    """
    hook = DatabricksHookAsync()
    hook.databricks_conn = await sync_to_async(hook.get_connection)(
        hook.databricks_conn_id
    )
    hook.databricks_conn.login = LOGIN
    hook.databricks_conn.password = PASSWORD
    json = {
        "task_id": TASK_ID,
        "existing_cluster_id": "xxxx-xxxxxx-xxxxxx",
        "notebook_task": {"notebook_path": "/Users/test@astronomer.io/test_notebook"},
    }

    with pytest.raises(AirflowException) as exc:
        await hook._do_api_call_async(("NOPE", "api/2.0/jobs/runs/submit"), json)

    assert str(exc.value) == "Unexpected HTTP Method: NOPE"
