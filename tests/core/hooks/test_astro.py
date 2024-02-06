from unittest.mock import MagicMock, patch

import pytest
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
                "password": "ey...xz.ey...fq.tw...ap",
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
