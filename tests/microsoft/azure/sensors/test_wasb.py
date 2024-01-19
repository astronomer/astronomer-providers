import pytest
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor, WasbPrefixSensor

from astronomer.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensorAsync,
    WasbPrefixSensorAsync,
)

TEST_DATA_STORAGE_BLOB_NAME = "test_blob_providers_team.txt"
TEST_DATA_STORAGE_CONTAINER_NAME = "test-container-providers-team"
TEST_DATA_STORAGE_BLOB_PREFIX = TEST_DATA_STORAGE_BLOB_NAME[:10]

MODULE = "astronomer.providers.microsoft.azure.sensors.wasb"


class TestWasbBlobSensorAsync:
    def test_init(self):
        task = WasbBlobSensorAsync(
            task_id="wasb_blob_sensor_async",
            container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
            blob_name=TEST_DATA_STORAGE_BLOB_NAME,
        )

        assert isinstance(task, WasbBlobSensor)
        assert task.deferrable is True

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
    def test_init(self):
        task = WasbPrefixSensorAsync(
            task_id="wasb_prefix_sensor_async",
            container_name=TEST_DATA_STORAGE_CONTAINER_NAME,
            prefix=TEST_DATA_STORAGE_BLOB_PREFIX,
        )

        assert isinstance(task, WasbPrefixSensor)
        assert task.deferrable is True

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
