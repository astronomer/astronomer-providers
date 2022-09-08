import json
from unittest import mock
from unittest.mock import AsyncMock

import multidict
import pytest
from aiohttp import ClientResponseError, RequestInfo
from airflow import AirflowException
from airflow.models.connection import Connection

from astronomer.providers.dbt.cloud.hooks.dbt import DbtCloudHookAsync
from astronomer.providers.package import get_provider_info

SAMPLE_RESPONSE = {
    "data": {"status": 1, "status_message": "Success"},
    "status": {"code": 200, "user_message": "string", "developer_message": "string"},
}
SAMPLE_RESPONSE_WITH_ERROR = {
    "data": {},
    "status": {
        "code": 401,
        "message": "Test message",
        "user_message": "string",
        "developer_message": "string",
    },
}
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Token newT0k3n",
    "User-Agent": "dbtcloud-v1.2",
}


class TestDbtCloudJobRunHookAsync:
    RUN_ID = 1234
    CONN_ID = "dbt_cloud_default"
    ACCOUNT_ID = 12340

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_job_details")
    async def test_get_job_status(self, mock_get_job_details):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_get_job_details.return_value = SAMPLE_RESPONSE
        hook = DbtCloudHookAsync(dbt_cloud_conn_id="test_conn")
        response = await hook.get_job_status(self.RUN_ID, self.ACCOUNT_ID)
        assert response == 1

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_job_details")
    async def test_get_job_status_exception(self, mock_get_job_details):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_get_job_details.side_effect = AirflowException("Bad request")
        hook = DbtCloudHookAsync(dbt_cloud_conn_id="test_conn")
        with pytest.raises(AirflowException):
            await hook.get_job_status(self.RUN_ID, self.ACCOUNT_ID)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_headers")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_request_url_params")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_connection")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.aiohttp.ClientSession.get")
    async def test_get_job_details(
        self, mock_get, mock_get_conn, mock_get_request_url_params, mock_get_headers
    ):
        mock_get_conn.return_value = Connection(
            conn_id=self.CONN_ID,
            conn_type="test",
            login=1234,
            password="newT0k3n",
            schema="Tenant",
            extra=json.dumps(
                {
                    "login": "test",
                    "password": "newT0k3n",
                    "schema": "Tenant",
                }
            ),
        )
        mock_get_headers.return_value = HEADERS
        mock_get_request_url_params.return_value = "/test/airflow/", {}
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=SAMPLE_RESPONSE)
        hook = DbtCloudHookAsync(dbt_cloud_conn_id=self.CONN_ID)
        response = await hook.get_job_details(self.RUN_ID)
        assert response == SAMPLE_RESPONSE

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_headers")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_request_url_params")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_connection")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.aiohttp.ClientSession.get")
    async def test_get_job_details_without_account_id_exception(
        self, mock_get, mock_get_conn, mock_get_request_url_params, mock_get_headers
    ):
        mock_get_conn.return_value = Connection(
            extra=json.dumps(
                {
                    "password": "newT0k3n",
                    "schema": "Tenant",
                }
            )
        )
        mock_get_headers.return_value = HEADERS
        mock_get_request_url_params.return_value = "/test/airflow/", {}
        mock_get.return_value.__aenter__.return_value.json = AsyncMock(return_value=SAMPLE_RESPONSE)
        hook = DbtCloudHookAsync(dbt_cloud_conn_id=self.CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_job_details(self.RUN_ID)

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_headers")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_request_url_params")
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.aiohttp.ClientSession.get")
    async def test_get_job_details_with_error(self, mock_get, mock_get_request_url_params, mock_get_headers):
        mock_get_headers.return_value = HEADERS
        mock_get_request_url_params.return_value = "/test/airflow/", {}
        mock_get.return_value.__aenter__.return_value.json.side_effect = ClientResponseError(
            request_info=RequestInfo(url="example.com", method="PATCH", headers=multidict.CIMultiDict()),
            status=500,
            history=[],
        )
        hook = DbtCloudHookAsync(dbt_cloud_conn_id="test_conn")
        with pytest.raises(AirflowException):
            await hook.get_job_details(self.RUN_ID, self.ACCOUNT_ID)

    @pytest.mark.parametrize(
        "mock_endpoint, mock_param, expected_url, expected_param",
        [
            ("account/1234/run", None, "http://localhost/account/1234/run", {}),
            (
                "/account/1234/run",
                ["test"],
                "http://localhost/account/1234/run",
                {"include_related": ["test"]},
            ),
        ],
    )
    def test_get_request_url_params(self, mock_endpoint, mock_param, expected_url, expected_param):
        """Test get_request_url_header_params by mocking _get_conn_params and get_headers"""
        hook = DbtCloudHookAsync(dbt_cloud_conn_id="test_conn")
        hook.base_url = "http://localhost"
        url, param = hook.get_request_url_params(mock_endpoint, mock_param)
        assert url == expected_url
        assert param == expected_param

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.dbt.cloud.hooks.dbt.DbtCloudHookAsync.get_connection")
    async def test_get_headers(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            conn_id=self.CONN_ID,
            conn_type="test",
            login=1234,
            password="newT0k3n",
            schema="Tenant",
            extra=json.dumps(
                {
                    "login": "test",
                    "password": "newT0k3n",
                    "schema": "Tenant",
                }
            ),
        )
        provider_info = get_provider_info()
        package_name = provider_info["package-name"]
        version = provider_info["versions"]
        HEADERS["User-Agent"] = f"{package_name}-v{version}"
        hook = DbtCloudHookAsync(dbt_cloud_conn_id=self.CONN_ID)
        resp = await hook.get_headers()
        assert resp == HEADERS
