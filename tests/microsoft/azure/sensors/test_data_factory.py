from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensorAsync,
)
from astronomer.providers.microsoft.azure.triggers.data_factory import (
    ADFPipelineRunStatusSensorTrigger,
)

RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
TASK_ID = "test_pipeline_run_sensor_async"


@pytest.fixture
def context():
    """Creates an empty context."""
    context = {}
    yield context


def test_adf_pipeline_status_sensor_async():
    """Assert execute method defer for Azure Data factory pipeline run status sensor"""
    task = AzureDataFactoryPipelineRunStatusSensorAsync(
        task_id="pipeline_run_sensor_async",
        run_id=RUN_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(
        exc.value.trigger, ADFPipelineRunStatusSensorTrigger
    ), "Trigger is not a ADFPipelineRunStatusSensorTrigger"


def test_adf_pipeline_status_sensor_execute_complete_success():
    """Assert execute_complete log success message when trigger fire with target status"""
    task = AzureDataFactoryPipelineRunStatusSensorAsync(
        task_id="pipeline_run_sensor_async",
        run_id=RUN_ID,
    )

    msg = f"Pipeline run {RUN_ID} has been succeeded."
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context={}, event={"status": "success", "message": msg})
    mock_log_info.assert_called_with(msg)


def test_adf_pipeline_status_sensor_execute_complete_failure():
    """Assert execute_complete method fail"""
    task = AzureDataFactoryPipelineRunStatusSensorAsync(
        task_id="pipeline_run_sensor_async",
        run_id=RUN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context={}, event={"status": "error", "message": ""})
