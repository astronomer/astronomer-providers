from unittest import mock
from unittest.mock import MagicMock, Mock, patch

import pytest
from aioresponses import aioresponses
from airflow.exceptions import AirflowException

from astronomer.providers.core.hooks.astro import AstroHook


class TestAstroHook:

    def test_get_ui_field_behaviour(self):
        hook = AstroHook()

        result = hook.get_ui_field_behaviour()

        expected_result = {
            "hidden_fields": ["login", "port", "schema", "extra"],
            "relabeling": {
                "password": "Astro Cloud API Token",
            },
            "placeholders": {
                "host": "https://clmkpsyfc010391acjie00t1l.astronomer.run/d5lc9c9x",
                "password": "Astro API JWT Token",
            },
        }

        assert result == expected_result

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("os.environ.get")
    def test_get_conn(self, mock_os_get, mock_get_connection):
        # Create an instance of your class
        hook = AstroHook()

        # Mock the return values for BaseHook.get_connection and os.environ.get
        mock_conn = MagicMock()
        mock_conn.host = "http://example.com"
        mock_conn.password = "your_api_token"
        mock_get_connection.return_value = mock_conn
        mock_os_get.return_value = "http://example.com"

        result = hook.get_conn()

        expected_result = ("http://example.com", "your_api_token")

        # Assert that the actual result matches the expected result
        assert result == expected_result

        mock_get_connection.assert_called_once_with(hook.astro_cloud_conn_id)

        # Reset the mocks
        mock_get_connection.reset_mock()
        mock_os_get.reset_mock()

        # Test case where conn.host is None
        mock_conn.host = None
        mock_os_get.return_value = None

        with pytest.raises(AirflowException):
            hook.get_conn()

        mock_get_connection.assert_called_once_with(hook.astro_cloud_conn_id)

        # Reset the mocks
        mock_get_connection.reset_mock()

        # Test case where conn.password is None
        mock_conn.host = "http://example.com"
        mock_conn.password = None

        with pytest.raises(AirflowException):
            hook.get_conn()

        mock_get_connection.assert_called_once_with(hook.astro_cloud_conn_id)

    @patch("astronomer.providers.core.hooks.astro.AstroHook.get_conn")
    def test_headers(self, mock_get_conn):
        # Create an instance of your class
        your_instance = AstroHook()

        # Mock the return value for the get_conn method
        mock_get_conn.return_value = ("http://example.com", "your_api_token")

        # Call the property and get the result
        result = your_instance._headers

        # Define the expected result based on the method implementation
        expected_result = {"accept": "application/json", "Authorization": "Bearer your_api_token"}

        # Assert that the actual result matches the expected result
        assert result == expected_result

        # Assert that get_conn was called once
        mock_get_conn.assert_called_once()

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("astronomer.providers.core.hooks.astro.requests.get")
    def test_get_dag_runs(self, mock_requests_get, mock_get_connection):
        hook = AstroHook()

        # Mocking the response from requests.get
        mock_response = MagicMock()
        mock_response.json.return_value = {"dag_runs": [{"dag_run_id": "123", "state": "running"}]}
        mock_requests_get.return_value = mock_response

        # Calling the method to be tested
        result = hook.get_dag_runs("external_dag_id")

        # Assertions
        mock_requests_get.assert_called_once()
        assert result == [{"dag_run_id": "123", "state": "running"}]

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("astronomer.providers.core.hooks.astro.requests.get")
    def test_get_dag_run(self, mock_requests_get, mock_get_connection):
        hook = AstroHook()

        # Mocking the response from requests.get
        mock_response = MagicMock()
        mock_response.json.return_value = {"dag_run_id": "123", "state": "running"}
        mock_requests_get.return_value = mock_response

        # Calling the method to be tested
        result = hook.get_dag_run("external_dag_id", "123")

        # Assertions
        mock_requests_get.assert_called_once()
        assert result == {"dag_run_id": "123", "state": "running"}

    @patch("airflow.hooks.base.BaseHook.get_connection")
    @patch("astronomer.providers.core.hooks.astro.requests.get")
    def test_get_task_instance(self, mock_requests_get, mock_get_connection):
        hook = AstroHook()

        # Mocking the response from requests.get
        mock_response = MagicMock()
        mock_response.json.return_value = {"task_instance_id": "456", "state": "success"}
        mock_requests_get.return_value = mock_response

        # Calling the method to be tested
        result = hook.get_task_instance("external_dag_id", "123", "external_task_id")

        # Assertions
        mock_requests_get.assert_called_once()
        assert result == {"task_instance_id": "456", "state": "success"}

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.core.hooks.astro.AstroHook._headers")
    async def test_get_a_dag_run(self, mock_headers):
        external_dag_id = "your_external_dag_id"
        dag_run_id = "your_dag_run_id"
        url = f"https://test.com/api/v1/dags/{external_dag_id}/dagRuns/{dag_run_id}"

        # Mocking necessary objects
        your_class_instance = AstroHook()
        your_class_instance.get_conn = Mock(return_value=("https://test.com", "Test Token"))
        mock_headers.return_value = {"accept": "application/json", "Authorization": "Bearer Token"}
        response_data = {
            "conf": {},
            "dag_id": "my_dag",
            "dag_run_id": "manual__2024-02-14T19:06:32.053905+00:00",
            "data_interval_end": "2024-02-14T19:06:32.053905+00:00",
            "data_interval_start": "2024-02-14T19:06:32.053905+00:00",
            "end_date": "2024-02-14T19:16:33.987139+00:00",
            "execution_date": "2024-02-14T19:06:32.053905+00:00",
            "external_trigger": True,
            "last_scheduling_decision": "2024-02-14T19:16:33.985973+00:00",
            "logical_date": "2024-02-14T19:06:32.053905+00:00",
            "note": None,
            "run_type": "manual",
            "start_date": "2024-02-14T19:06:33.004299+00:00",
            "state": "success",
        }

        with aioresponses() as mock_session:
            mock_session.get(
                url,
                headers=your_class_instance._headers,
                status=200,
                payload=response_data,
            )

            result = await your_class_instance.get_a_dag_run(external_dag_id, dag_run_id)

        assert result == response_data

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.core.hooks.astro.AstroHook._headers")
    async def test_get_a_task_instance(self, mock_headers):
        external_dag_id = "your_external_dag_id"
        dag_run_id = "your_dag_run_id"
        external_task_id = "your_external_task_id"
        url = f"https://test.com/api/v1/dags/{external_dag_id}/dagRuns/{dag_run_id}/taskInstances/{external_task_id}"

        # Mocking necessary objects
        your_class_instance = AstroHook()
        your_class_instance.get_conn = Mock(return_value=("https://test.com", "Test Token"))
        mock_headers.return_value = {"accept": "application/json", "Authorization": "Bearer Token"}
        response_data = {
            "dag_id": "my_dag",
            "dag_run_id": "manual__2024-02-14T19:06:32.053905+00:00",
            "duration": 600.233105,
            "end_date": "2024-02-14T19:16:33.459676+00:00",
            "execution_date": "2024-02-14T19:06:32.053905+00:00",
            "executor_config": "{}",
            "hostname": "d10fc8b0ad27",
            "map_index": -1,
            "max_tries": 0,
            "note": None,
            "operator": "_PythonDecoratedOperator",
            "pid": 927,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_when": "2024-02-14T19:06:33.036108+00:00",
            "rendered_fields": {"op_args": [], "op_kwargs": {}, "templates_dict": None},
            "sla_miss": None,
            "start_date": "2024-02-14T19:06:33.226571+00:00",
            "state": "success",
            "task_id": "my_python_function",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 1,
            "unixname": "astro",
        }

        with aioresponses() as mock_session:
            mock_session.get(
                url,
                headers=your_class_instance._headers,
                status=200,
                payload=response_data,
            )

            result = await your_class_instance.get_a_task_instance(
                external_dag_id, dag_run_id, external_task_id
            )

        assert result == response_data
