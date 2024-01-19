import pytest
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor

from astronomer.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensorAsync,
)


class TestAzureDataFactoryPipelineRunStatusSensorAsync:
    RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"

    def test_init(self):
        task = AzureDataFactoryPipelineRunStatusSensorAsync(
            task_id="pipeline_run_sensor_async",
            run_id=self.RUN_ID,
            factory_name="factory_name",
            resource_group_name="resource_group_name",
        )

        assert isinstance(task, AzureDataFactoryPipelineRunStatusSensor)
        assert task.deferrable is True

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for AzureDataFactoryPipelineRunStatusSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            AzureDataFactoryPipelineRunStatusSensorAsync(
                task_id="pipeline_run_sensor_async",
                run_id=self.RUN_ID,
                poll_interval=5.0,
                factory_name="factory_name",
                resource_group_name="resource_group_name",
            )
