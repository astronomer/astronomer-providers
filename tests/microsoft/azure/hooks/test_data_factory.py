import json
from unittest import mock

import pytest
from airflow import AirflowException
from airflow.models.connection import Connection
from azure.mgmt.datafactory.aio import DataFactoryManagementClient

from astronomer.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHookAsync,
)

AZURE_DATA_FACTORY_CONN_ID = "azure_data_factory_default"
RESOURCE_GROUP_NAME = "team_provider_resource_group_test"
DATAFACTORY_NAME = "ADFProvidersTeamDataFactory"
TASK_ID = "test_adf_pipeline_run_status_sensor"
RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"

DEFAULT_CONNECTION_CLIENT_SECRET = "azure_data_factory_test_client_secret"
DEFAULT_CONNECTION_DEFAULT_CREDENTIAL = "azure_data_factory_test_default_credential"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_status",
    ["Queued", "InProgress", "Succeeded", "Failed", "Cancelled"],
)
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn"
)
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_pipeline_run"
)
async def test_get_adf_pipeline_run_status(mock_get_pipeline_run, mock_conn, mock_status):
    """Test get_adf_pipeline_run_status function with mocked status"""
    mock_get_pipeline_run.return_value.status = mock_status
    hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
    response = await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
    assert response == mock_status


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn"
)
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_pipeline_run"
)
async def test_get_adf_pipeline_run_status_exception(mock_get_pipeline_run, mock_conn):
    """Test get_adf_pipeline_run_status function with exception"""
    mock_get_pipeline_run.side_effect = Exception("Test exception")
    hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
    with pytest.raises(AirflowException):
        await hook.get_adf_pipeline_run_status(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)


@pytest.mark.asyncio
@mock.patch("azure.mgmt.datafactory.models._models_py3.PipelineRun")
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn"
)
async def test_get_pipeline_run(mock_async_connection, mock_pipeline_run):
    """Test get_pipeline_run run function"""
    mock_async_connection.return_value.__aenter__.return_value.pipeline_runs.get.return_value = (
        mock_pipeline_run
    )
    hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
    response = await hook.get_pipeline_run(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
    assert response == mock_pipeline_run


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_async_conn"
)
async def test_get_pipeline_run_exception(mock_conn):
    """Test get_pipeline_run function with exception"""
    mock_conn.return_value.__aenter__.return_value.pipeline_runs.get.side_effect = Exception("Test exception")
    hook = AzureDataFactoryHookAsync(AZURE_DATA_FACTORY_CONN_ID)
    with pytest.raises(AirflowException):
        await hook.get_pipeline_run(RUN_ID, RESOURCE_GROUP_NAME, DATAFACTORY_NAME)


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_connection"
)
async def test_get_async_conn(mock_connection):
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
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_connection"
)
async def test_get_async_conn_without_login_id(mock_connection):
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
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_connection"
)
async def test_get_async_conn_key_error(mock_connection, mock_connection_params):
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
