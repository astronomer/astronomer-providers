from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensorAsync,
    WasbPrefixSensorAsync,
)
from astronomer.providers.microsoft.azure.triggers.wasb import (
    WasbBlobSensorTrigger,
    WasbPrefixSensorTrigger,
)
from tests.utils.airflow_util import create_context

TEST_DATA_STORAGE_BLOB_NAME = "test_blob_providers_team.txt"
TEST_DATA_STORAGE_CONTAINER_NAME = "test-container-providers-team"
TEST_DATA_STORAGE_BLOB_PREFIX = TEST_DATA_STORAGE_BLOB_NAME[:10]

MODULE = "astronomer.providers.microsoft.azure.sensors.wasb"


class TestWasbBlobSensorAsync:
    SENSOR = WasbBlobSensorAsync(
        task_id="wasb_blob_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        blob_name=TEST_DATA_STORAGE_BLOB_NAME,
    )

    @mock.patch(f"{MODULE}.WasbBlobSensorAsync.defer")
    @mock.patch(f"{MODULE}.WasbBlobSensorAsync.poke", return_value=True)
    def test_wasb_blob_sensor_async_finish_before_deferred(self, mock_poke, mock_defer, context):
        """Assert task is not deferred when it receives a finish status before deferring"""
        self.SENSOR.execute(create_context(self.SENSOR))
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.WasbBlobSensorAsync.poke", return_value=False)
    def test_wasb_blob_sensor_async(self, mock_poke):
        """Assert execute method defer for wasb blob sensor"""

        with pytest.raises(TaskDeferred) as exc:
            self.SENSOR.execute(create_context(self.SENSOR))
        assert isinstance(exc.value.trigger, WasbBlobSensorTrigger), "Trigger is not a WasbBlobSensorTrigger"

    @pytest.mark.parametrize(
        "event",
        [{"status": "success", "message": "Job completed"}],
    )
    def test_wasb_blob_sensor_execute_complete_success(self, event):
        """Assert execute_complete log success message when trigger fire with target status."""

        with mock.patch.object(self.SENSOR.log, "info") as mock_log_info:
            self.SENSOR.execute_complete(context={}, event=event)
        mock_log_info.assert_called_with(event["message"])

    def test_wasb_blob_sensor_execute_complete_failure(self):
        """Assert execute_complete method raises an exception when the triggerer fires an error event."""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(context={}, event={"status": "error", "message": ""})

    def test_poll_interval_deprecation_warning_wasb_blob(self):
        """Test DeprecationWarning for WasbBlobSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            WasbBlobSensorAsync(
                task_id="wasb_blob_sensor_async",
                container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
                blob_name=TEST_DATA_STORAGE_BLOB_NAME,
                poll_interval=5.0,
            )


class TestWasbPrefixSensorAsync:
    SENSOR = WasbPrefixSensorAsync(
        task_id="wasb_prefix_sensor_async",
        container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
        prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
    )

    @mock.patch(f"{MODULE}.WasbPrefixSensorAsync.defer")
    @mock.patch(f"{MODULE}.WasbPrefixSensorAsync.poke", return_value=True)
    def test_wasb_prefix_sensor_async_finish_before_deferred(self, mock_poke, mock_defer, context):
        """Assert task is not deferred when it receives a finish status before deferring"""
        self.SENSOR.execute(create_context(self.SENSOR))

        assert not mock_defer.called

    @mock.patch(f"{MODULE}.WasbPrefixSensorAsync.poke", return_value=False)
    def test_wasb_prefix_sensor_async(self, mock_poke):
        """Assert execute method defer for wasb prefix sensor"""

        with pytest.raises(TaskDeferred) as exc:
            self.SENSOR.execute(create_context(self.SENSOR))
        assert isinstance(
            exc.value.trigger, WasbPrefixSensorTrigger
        ), "Trigger is not a WasbPrefixSensorTrigger"

    @pytest.mark.parametrize(
        "event",
        [{"status": "success", "message": "Job completed"}],
    )
    def test_wasb_prefix_sensor_execute_complete_success(self, event):
        """Assert execute_complete log success message when trigger fire with target status."""

        with mock.patch.object(self.SENSOR.log, "info") as mock_log_info:
            self.SENSOR.execute_complete(context={}, event=event)
        mock_log_info.assert_called_with(event["message"])

    def test_wasb_prefix_sensor_execute_complete_failure(self):
        """Assert execute_complete method raises an exception when the triggerer fires an error event."""

        with pytest.raises(AirflowException):
            self.SENSOR.execute_complete(context={}, event={"status": "error", "message": ""})

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for WasbPrefixSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            WasbPrefixSensorAsync(
                task_id="wasb_prefix_sensor_async",
                container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
                prefix=TEST_DATA_STORAGE_BLOB_NAME,
                poll_interval=5.0,
            )
