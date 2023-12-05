import logging
from unittest import mock
from unittest.mock import MagicMock

import pytest
from aiohttp import ClientConnectorError
from airflow import __version__ as provider_version
from airflow.exceptions import AirflowException
from airflow.providers.databricks.hooks.databricks import (
    GET_RUN_ENDPOINT,
    SUBMIT_RUN_ENDPOINT,
)
from packaging import version

from astronomer.providers.databricks.hooks.databricks import DatabricksHookAsync

TASK_ID = "databricks_check"
CONN_ID = "unit_test_conn_id"
RUN_ID = "unit_test_run_id"
LOGIN = "login"
PASSWORD = "password"
TOKEN = "token"
api_version = "2.0"
MOCK_GET_OUTPUT_RESPONSE = {
    "metadata": {
        "job_id": 11223344,
        "run_id": 455644833,
        "number_in_job": 455644833,
        "state": {
            "life_cycle_state": "TERMINATED",
            "result_state": "FAILED",
            "state_message": "failed with error",
        },
        "tasks": [
            {
                "run_id": 2112892,
                "task_key": "Orders_Ingest",
                "description": "Ingests order data",
                "job_cluster_key": "auto_scaling_cluster",
                "spark_jar_task": {"main_class_name": "com.databricks.OrdersIngest", "run_as_repl": True},
                "libraries": [{"jar": "dbfs:/mnt/databricks/OrderIngest.jar"}],
                "state": {
                    "life_cycle_state": "INTERNAL_ERROR",
                    "result_state": "FAILED",
                    "user_cancelled_or_timedout": False,
                },
                "run_page_url": "https://my-workspace.cloud.databricks.com/#job/39832/run/20",
                "start_time": 1629989929660,
                "setup_duration": 0,
                "execution_duration": 0,
                "cleanup_duration": 0,
                "end_time": 1629989930171,
                "cluster_instance": {
                    "cluster_id": "0923-164208-meows279",
                    "spark_context_id": "4348585301701786933",
                },
                "attempt_number": 0,
            }
        ],
    }
}

# For provider version > 2.0.2 GET_RUN_ENDPOINT and SUBMIT_RUN_ENDPOINT points to api/2.1 instead of api/2.0
if version.parse(provider_version) > version.parse("2.0.2"):
    api_version = "2.1"


class TestDatabricksHookAsync:
    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync._do_api_call_async")
    async def test_databricks_hook_get_run_state(self, mocked_response):
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
    async def test_do_api_call_async_get_basic_auth(self, caplog, aioresponse):
        """
        Asserts that the Databricks hook makes a GET call as expected when
        provided with basic auth credentials.
        The aiohttp module assumes basic auth for its 'auth' parameter, so
        we need to set this manually in the header for both bearer token
        and basic auth.
        """
        caplog.set_level(logging.INFO)
        hook = DatabricksHookAsync()
        hook.databricks_conn = MagicMock()
        hook.databricks_conn.host = "https://localhost"
        hook.databricks_conn.login = LOGIN
        hook.databricks_conn.password = PASSWORD
        params = {"run_id": RUN_ID}

        aioresponse.get(
            f"https://localhost/api/{api_version}/jobs/runs/get?run_id=unit_test_run_id",
            status=200,
            body='{"result":"Yay!"}',
        )
        resp = await hook._do_api_call_async(GET_RUN_ENDPOINT, params)
        assert resp == {"result": "Yay!"}
        assert resp["result"] == "Yay!"
        assert "Using basic auth. " in caplog.text

    @pytest.mark.asyncio
    async def test_do_api_call_async_get_auth_token(self, caplog, aioresponse):
        """
        Asserts that the Databricks hook makes a GET call as expected when
        provided with an auth token.
        The aiohttp module assumes basic auth for its 'auth' parameter, so
        we need to set this manually in the header for both bearer token
        and basic auth.
        """
        caplog.set_level(logging.INFO)
        hook = DatabricksHookAsync()
        hook.databricks_conn = MagicMock()
        hook.databricks_conn.host = "https://localhost"
        hook.databricks_conn.extra_dejson = {"token": "test_token"}
        params = {"run_id": RUN_ID}

        aioresponse.get(
            f"https://localhost/api/{api_version}/jobs/runs/get?run_id=unit_test_run_id",
            status=200,
            body='{"result":"Yay!"}',
        )
        resp = await hook._do_api_call_async(GET_RUN_ENDPOINT, params)
        assert resp == {"result": "Yay!"}
        assert resp["result"] == "Yay!"
        assert "Using token auth. " in caplog.text

    @pytest.mark.asyncio
    async def test_do_api_call_async_non_retryable_error(self, aioresponse):
        """
        Asserts that the Databricks hook will throw an exception
        when a non-retryable error is returned by the API.
        """
        hook = DatabricksHookAsync()
        hook.databricks_conn = MagicMock()
        hook.databricks_conn.host = "https://localhost"
        hook.databricks_conn.login = LOGIN
        hook.databricks_conn.password = PASSWORD
        params = {"run_id": RUN_ID}

        aioresponse.get(
            f"https://localhost/api/{api_version}/jobs/runs/get?run_id=unit_test_run_id",
            status=400,
        )

        with pytest.raises(AirflowException):
            await hook._do_api_call_async(GET_RUN_ENDPOINT, params)

    @pytest.mark.parametrize("status_code", (500, 503, 403))
    @pytest.mark.asyncio
    async def test_do_api_call_async_retryable_error(self, aioresponse, status_code):
        """
        Asserts that the Databricks hook will attempt another API call as many
        times as the retry_limit when a retryable error is returned by the API.
        """
        hook = DatabricksHookAsync()
        hook.databricks_conn = MagicMock()
        hook.databricks_conn.host = "https://localhost"
        hook.databricks_conn.login = LOGIN
        hook.databricks_conn.password = PASSWORD
        params = {"run_id": RUN_ID}

        aioresponse.get(
            f"https://localhost/api/{api_version}/jobs/runs/get?run_id=unit_test_run_id",
            status=status_code,
            repeat=True,
        )

        with pytest.raises(AirflowException) as exc:
            await hook._do_api_call_async(GET_RUN_ENDPOINT, params)

        assert str(exc.value) == f"API requests to Databricks failed {hook.retry_limit} times. Giving up."

    @pytest.mark.asyncio
    async def test_do_api_call_async_post(self, aioresponse):
        """Asserts that the Databricks hook makes a POST call as expected."""
        hook = DatabricksHookAsync()
        hook.databricks_conn = MagicMock()
        hook.databricks_conn.host = "https://localhost"
        hook.databricks_conn.login = LOGIN
        hook.databricks_conn.password = PASSWORD
        json = {
            "task_id": TASK_ID,
            "existing_cluster_id": "xxxx-xxxxxx-xxxxxx",
            "notebook_task": {"notebook_path": "/Users/test@astronomer.io/test_notebook"},
        }

        aioresponse.post(
            f"https://localhost/api/{api_version}/jobs/runs/submit",
            status=202,
            body='{"result":"Yay!"}',
        )
        resp = await hook._do_api_call_async(SUBMIT_RUN_ENDPOINT, json)
        assert resp == {"result": "Yay!"}
        assert resp["result"] == "Yay!"

    @pytest.mark.asyncio
    async def test_do_api_call_async_unknown_method(self):
        """
        Asserts that the Databricks hook throws an exception when it attempts to
        make an API call using a non-existent method.
        """
        hook = DatabricksHookAsync()
        hook.databricks_conn = MagicMock()
        hook.databricks_conn.host = "https://localhost"
        hook.databricks_conn.login = LOGIN
        hook.databricks_conn.password = PASSWORD
        payload = {
            "task_id": TASK_ID,
            "existing_cluster_id": "xxxx-xxxxxx-xxxxxx",
            "notebook_task": {"notebook_path": "/Users/test@astronomer.io/test_notebook"},
        }

        with pytest.raises(AirflowException) as exc:
            await hook._do_api_call_async(("NOPE", "api/2.0/jobs/runs/submit"), payload)

        assert str(exc.value) == "Unexpected HTTP Method: NOPE"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_response",
        [
            (
                {
                    "state": {
                        "life_cycle_state": "RUNNING",
                        "result_state": "",
                        "state_message": "In run",
                        "user_cancelled_or_timedout": "False",
                    }
                }
            ),
            (
                {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                        "state_message": "",
                        "user_cancelled_or_timedout": "False",
                    }
                }
            ),
        ],
    )
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync._do_api_call_async")
    async def test_get_run_response(self, mock_do_api_async, mock_response):
        """
        Test get_run_response function by mocking the _do_api_call_async response and the response date from the API.
        """
        hook = DatabricksHookAsync()
        mock_do_api_async.return_value = mock_response
        run_state_response = await hook.get_run_response(RUN_ID)
        assert run_state_response == mock_response

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.databricks.hooks.databricks.DatabricksHookAsync._do_api_call_async")
    async def test_get_run_output_response(self, mock_do_api_async):
        """Test get_run_output_response method by mocking _do_api_call_async and the response of get-output API"""
        hook = DatabricksHookAsync()
        mock_do_api_async.return_value = MOCK_GET_OUTPUT_RESPONSE
        run_output = await hook.get_run_output_response(RUN_ID)
        assert run_output == MOCK_GET_OUTPUT_RESPONSE

    def test___retryable_error_async_with_client_connector_error(self):
        exception = ClientConnectorError(connection_key="", os_error=OSError())
        assert DatabricksHookAsync._retryable_error_async(exception) is True
