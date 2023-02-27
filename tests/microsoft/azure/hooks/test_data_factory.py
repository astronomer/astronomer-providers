import json
from unittest import mock

import pytest
from airflow import AirflowException
from airflow.models.connection import Connection
from azure.mgmt.datafactory.aio import DataFactoryManagementClient

from astronomer.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHookAsync, get_field

DEFAULT_RESOURCE_GROUP = "defaultResourceGroup"
AZURE_DATA_FACTORY_CONN_ID = "azure_data_factory_default"
RESOURCE_GROUP_NAME = "team_provider_resource_group_test"
DATAFACTORY_NAME = "ADFProvidersTeamDataFactory"
TASK_ID = "test_adf_pipeline_run_status_sensor"
RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
DEFAULT_CONNECTION_CLIENT_SECRET = "azure_data_factory_test_client_secret"
MODULE = "astronomer.providers.microsoft.azure"


class TestAzureDataFactoryHookAsync:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_status",
        ["Queued", "InProgress", "Succeeded", "Failed", "Cancelled"],
    )
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_pipeline_run")
    async def test_get_adf_pipeline_run_status(self, mock_get_pipeline_run, mock_conn, mock_status):
        """Test get_adf_pipeline_run_status function with mocked status"""
        mock_get_pipeline_run.return_value.status = mock_status
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_status

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_pipeline_run")
    async def test_get_adf_pipeline_run_status_exception(self, mock_get_pipeline_run, mock_conn):
        """Test get_adf_pipeline_run_status function with exception"""
        mock_get_pipeline_run.side_effect = Exception("Test exception")
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)

    @pytest.mark.asyncio
    @mock.patch("azure.mgmt.datafactory.models._models_py3.PipelineRun")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn")
    async def test_get_pipeline_run(self, mock_async_connection, mock_pipeline_run):
        """Test get_pipeline_run run function"""
        mock_async_connection.return_value.__aenter__.return_value.pipeline_runs.get.return_value = (
            mock_pipeline_run
        )
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_pipeline_run(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        assert response == mock_pipeline_run

    @pytest.mark.asyncio
    @mock.patch("azure.mgmt.datafactory.models._models_py3.PipelineRun")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_connection")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn")
    async def test_get_pipeline_run_without_resource_name(
        self, mock_async_connection, mock_get_connection, mock_pipeline_run
    ):
        """Test get_pipeline_run run function without passing the resource name to check the decorator function"""
        mock_connection = Connection(
            extra=json.dumps(
                {
                    "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                    "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
                }
            )
        )
        mock_get_connection.return_value = mock_connection
        mock_async_connection.return_value.__aenter__.return_value.pipeline_runs.get.return_value = (
            mock_pipeline_run
        )
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_pipeline_run(RUN_ID, None, DATAFACTORY_NAME)
        assert response == mock_pipeline_run

    @pytest.mark.asyncio
    @mock.patch("azure.mgmt.datafactory.models._models_py3.PipelineRun")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_connection")
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn")
    async def test_get_pipeline_run_exception_without_resource(
        self, mock_conn, mock_get_connection, mock_pipeline_run
    ):
        """
        Test get_pipeline_run function without passing the resource name to check the decorator function and
        raise exception
        """
        mock_connection = Connection(
            extra=json.dumps({"extra__azure_data_factory__factory_name": DATAFACTORY_NAME})
        )
        mock_get_connection.return_value = mock_connection
        mock_conn.return_value.__aenter__.return_value.pipeline_runs.get.return_value = mock_pipeline_run
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_pipeline_run(RUN_ID, None, DATAFACTORY_NAME)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn")
    async def test_get_pipeline_run_exception(self, mock_conn):
        """Test get_pipeline_run function with exception"""
        mock_conn.return_value.__aenter__.return_value.pipeline_runs.get.side_effect = Exception(
            "Test exception"
        )
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(AirflowException):
            await hook.get_pipeline_run(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_connection")
    async def test_get_async_conn(self, mock_connection):
        """"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra=json.dumps(
                {
                    "extra__azure_data_factory__tenantId": "tenantId",
                    "extra__azure_data_factory__subscriptionId": "subscriptionId",
                    "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                    "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
                }
            ),
        )
        mock_connection.return_value = mock_conn
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_async_conn()
        assert isinstance(response, DataFactoryManagementClient)

    @pytest.mark.asyncio
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_connection")
    async def test_get_async_conn_without_login_id(self, mock_connection):
        """Test get_async_conn function without login id"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            extra=json.dumps(
                {
                    "extra__azure_data_factory__tenantId": "tenantId",
                    "extra__azure_data_factory__subscriptionId": "subscriptionId",
                    "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                    "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
                }
            ),
        )
        mock_connection.return_value = mock_conn
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        response = await hook.get_async_conn()
        assert isinstance(response, DataFactoryManagementClient)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_connection_params",
        [
            {
                "extra__azure_data_factory__tenantId": "tenantId",
                "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
            },
            {
                "extra__azure_data_factory__subscriptionId": "subscriptionId",
                "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
            },
        ],
    )
    @mock.patch(f"{MODULE}.hooks.data_factory.AzureDataFactoryHookAsync.get_connection")
    async def test_get_async_conn_key_error(self, mock_connection, mock_connection_params):
        """Test get_async_conn function with raising key error"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            login="clientId",
            password="clientSecret",
            extra=json.dumps(mock_connection_params),
        )
        mock_connection.return_value = mock_conn
        hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
        with pytest.raises(ValueError):
            await hook.get_async_conn()

    def test_get_field_prefixed_extras(self):
        """Test get_field function for retrieving prefixed extra fields"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            extra=json.dumps(
                {
                    "extra__azure_data_factory__tenantId": "tenantId",
                    "extra__azure_data_factory__subscriptionId": "subscriptionId",
                    "extra__azure_data_factory__resource_group_name": RESOURCE_GROUP_NAME,
                    "extra__azure_data_factory__factory_name": DATAFACTORY_NAME,
                }
            ),
        )
        extras = mock_conn.extra_dejson
        assert get_field(extras, "tenantId", strict=True) == "tenantId"
        assert get_field(extras, "subscriptionId", strict=True) == "subscriptionId"
        assert get_field(extras, "resource_group_name", strict=True) == RESOURCE_GROUP_NAME
        assert get_field(extras, "factory_name", strict=True) == DATAFACTORY_NAME
        with pytest.raises(ValueError):
            get_field(extras, "extra__azure_data_factory__tenantId", strict=True)
        with pytest.raises(KeyError):
            get_field(extras, "non-existent-field", strict=True)

    def test_get_field_non_prefixed_extras(self):
        """Test get_field function for retrieving non-prefixed extra fields"""
        mock_conn = Connection(
            conn_id=DEFAULT_CONNECTION_CLIENT_SECRET,
            conn_type="azure_data_factory",
            extra=json.dumps(
                {
                    "tenantId": "tenantId",
                    "subscriptionId": "subscriptionId",
                    "resource_group_name": RESOURCE_GROUP_NAME,
                    "factory_name": DATAFACTORY_NAME,
                }
            ),
        )
        extras = mock_conn.extra_dejson
        assert get_field(extras, "tenantId", strict=True) == "tenantId"
        assert get_field(extras, "subscriptionId", strict=True) == "subscriptionId"
        assert get_field(extras, "resource_group_name", strict=True) == RESOURCE_GROUP_NAME
        assert get_field(extras, "factory_name", strict=True) == DATAFACTORY_NAME
        with pytest.raises(ValueError):
            get_field(extras, "extra__azure_data_factory__tenantId", strict=True)
        with pytest.raises(KeyError):
            get_field(extras, "non-existent-field", strict=True)
