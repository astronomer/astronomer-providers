from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensorAsync,
)
from astronomer.providers.microsoft.azure.triggers.data_factory import ADFPipelineRunStatusSensorTrigger
from tests.utils.airflow_util import create_context


class TestAzureDataFactoryPipelineRunStatusSensorAsync:
    RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"
    SENSOR = AzureDataFactoryPipelineRunStatusSensorAsync(
        task_id="pipeline_run_sensor_async",
        run_id=RUN_ID,
    )

    def test_adf_pipeline_status_sensor_async(self):
        """Assert execute method defer for Azure Data factory pipeline run status sensor"""

        with pytest.raises(TaskDeferred) as exc:
            self.SENSOR.execute(create_context(self.SENSOR))
        assert isinstance(
            exc.value.trigger, ADFPipelineRunStatusSensorTrigger
        ), "Trigger is not a ADFPipelineRunStatusSensorTrigger"

    def test_adf_pipeline_status_sensor_execute_complete_success(self):
        """Assert execute_complete log success message when trigger fire with target status"""

        msg = f"Pipeline run {self.RUN_ID} has been succeeded."
        with mock.patch.object(self.SENSOR.log, "info") as mock_log_info:
            self.SENSOR.execute_complete(context={}, event={"status": "success", "message": msg})
        mock_log_info.assert_called_with(msg)

    def test_adf_pipeline_status_sensor_execute_complete_failure(self):
        """Assert execute_complete method fail"""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(context={}, event={"status": "error", "message": ""})

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for AzureDataFactoryPipelineRunStatusSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            AzureDataFactoryPipelineRunStatusSensorAsync(
                task_id="pipeline_run_sensor_async", run_id=self.RUN_ID, poll_interval=5.0
            )
