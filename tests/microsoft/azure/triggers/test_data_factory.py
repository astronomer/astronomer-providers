import asyncio
from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.microsoft.azure.triggers.data_factory import (
    ADFPipelineRunStatusSensorTrigger,
)

RESOURCE_GROUP_NAME = "team_provider_resource_group_test"
DATAFACTORY_NAME = "ADFProvidersTeamDataFactory"
TASK_ID = "test_adf_pipeline_run_status_sensor"
AZURE_DATA_FACTORY_CONN_ID = "azure_data_factory_default"
RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
POLL_INTERVAL = 5


def test_adf_pipeline_run_status_sensors_trigger_serialization():
    """
    Asserts that the TaskStateTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = ADFPipelineRunStatusSensorTrigger(
        run_id=RUN_ID,
        azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        factory_name=DATAFACTORY_NAME,
        poll_interval=POLL_INTERVAL,
    )
    classpath, kwargs = trigger.serialize()
    assert (
        classpath
        == "astronomer.providers.microsoft.azure.triggers.data_factory.ADFPipelineRunStatusSensorTrigger"
    )
    assert kwargs == {
        "run_id": RUN_ID,
        "azure_data_factory_conn_id": AZURE_DATA_FACTORY_CONN_ID,
        "resource_group_name": RESOURCE_GROUP_NAME,
        "factory_name": DATAFACTORY_NAME,
        "poll_interval": POLL_INTERVAL,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_status",
    [
        "Queued",
        "InProgress",
    ],
)
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_adf_pipeline_run_status"
)
async def test_adf_pipeline_run_status_sensors_trigger_run(mock_data_factory, mock_status):
    """
    Test if the task is run is in trigger successfully.
    """
    mock_data_factory.return_value = mock_status
    trigger = ADFPipelineRunStatusSensorTrigger(
        run_id=RUN_ID,
        azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        factory_name=DATAFACTORY_NAME,
        poll_interval=POLL_INTERVAL,
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)

    # TriggerEvent was not returned
    assert task.done() is False
    asyncio.get_event_loop().stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_status",
    ["Succeeded"],
)
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_adf_pipeline_run_status"
)
async def test_adf_pipeline_run_status_sensors_trigger_completed(mock_data_factory, mock_status):
    """
    Test if the task pipeline status is in succeeded status.
    :return:
    """
    mock_data_factory.return_value = mock_status
    trigger = ADFPipelineRunStatusSensorTrigger(
        run_id=RUN_ID,
        azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        factory_name=DATAFACTORY_NAME,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    msg = f"Pipeline run {RUN_ID} has been Succeeded."
    assert TriggerEvent({"status": "success", "message": msg}) in task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_status, mock_message",
    [
        ("Failed", f"Pipeline run {RUN_ID} has Failed."),
        ("Cancelled", f"Pipeline run {RUN_ID} has been Cancelled."),
    ],
)
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_adf_pipeline_run_status"
)
async def test_adf_pipeline_run_status_sensors_trigger_failure_status(
    mock_data_factory, mock_status, mock_message
):
    """
    Test if the task is run is in trigger failure status.
    :return:
    """
    mock_data_factory.return_value = mock_status
    trigger = ADFPipelineRunStatusSensorTrigger(
        run_id=RUN_ID,
        azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        factory_name=DATAFACTORY_NAME,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": mock_message}) in task


@pytest.mark.asyncio
@mock.patch(
    "astronomer.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHookAsync.get_adf_pipeline_run_status"
)
async def test_adf_pipeline_run_status_sensors_trigger_exception(mock_data_factory):
    """
    Test EMR container sensors with raise exception
    """
    mock_data_factory.side_effect = Exception("Test exception")
    trigger = ADFPipelineRunStatusSensorTrigger(
        run_id=RUN_ID,
        azure_data_factory_conn_id=AZURE_DATA_FACTORY_CONN_ID,
        resource_group_name=RESOURCE_GROUP_NAME,
        factory_name=DATAFACTORY_NAME,
        poll_interval=POLL_INTERVAL,
    )
    task = [i async for i in trigger.run()]
    assert len(task) == 1
    assert TriggerEvent({"status": "error", "message": "Test exception"}) in task
