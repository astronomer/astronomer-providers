from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensorAsync,
    GCSObjectsWithPrefixExistenceSensorAsync,
    GCSObjectUpdateSensorAsync,
    GCSUploadSessionCompleteSensorAsync,
)
from astronomer.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSCheckBlobUpdateTimeTrigger,
    GCSPrefixBlobTrigger,
    GCSUploadSessionTrigger,
)
from tests.utils.airflow_util import create_context

TEST_BUCKET = "TEST_BUCKET"
TEST_OBJECT = "TEST_OBJECT"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_INACTIVITY_PERIOD = 5
TEST_MIN_OBJECTS = 1

MODULE = "astronomer.providers.google.cloud.sensors.gcs"


class TestGCSObjectExistenceSensorAsync:
    OPERATOR = GCSObjectExistenceSensorAsync(
        task_id="gcs-object",
        bucket=TEST_BUCKET,
        object=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )

    def test_gcs_object_existence_sensor_async(self, context):
        """
        Asserts that a task is deferred and a GCSBlobTrigger will be fired
        when the GCSObjectExistenceSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(context)
        assert isinstance(exc.value.trigger, GCSBlobTrigger), "Trigger is not a GCSBlobTrigger"

    def test_gcs_object_existence_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_gcs_object_existence_sensor_async_execute_complete(self, context):
        """Asserts that logging occurs as expected"""

        with mock.patch.object(self.OPERATOR.log, "info") as mock_log_info:
            self.OPERATOR.execute_complete(
                context=context, event={"status": "success", "message": "Job completed"}
            )
        mock_log_info.assert_called_with("File %s was found in bucket %s.", TEST_OBJECT, TEST_BUCKET)


class TestGCSObjectsWithPrefixExistenceSensorAsync:
    OPERATOR = GCSObjectsWithPrefixExistenceSensorAsync(
        task_id="gcs-obj-prefix",
        bucket=TEST_BUCKET,
        prefix=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )

    def test_gcs_object_with_prefix_existence_sensor_async(self, context):
        """
        Asserts that a task is deferred and a GCSPrefixBlobTrigger will be fired
        when the GCSObjectsWithPrefixExistenceSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(context)
        assert isinstance(exc.value.trigger, GCSPrefixBlobTrigger), "Trigger is not a GCSPrefixBlobTrigger"

    def test_gcs_object_with_prefix_existence_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_gcs_object_with_prefix_existence_sensor_async_execute_complete(self, context):
        """Asserts that logging occurs as expected"""

        with mock.patch.object(self.OPERATOR.log, "info") as mock_log_info:
            self.OPERATOR.execute_complete(
                context=context,
                event={"status": "success", "message": "Job completed", "matches": [TEST_OBJECT]},
            )
        mock_log_info.assert_called_with(
            "Sensor checks existence of objects: %s, %s", TEST_BUCKET, TEST_OBJECT
        )

    def test_poll_interval_deprecation_warning_prefix_existence(self):
        """Test DeprecationWarning for GCSObjectsWithPrefixExistenceSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            GCSObjectsWithPrefixExistenceSensorAsync(
                task_id="task-id",
                bucket=TEST_BUCKET,
                prefix=TEST_OBJECT,
                google_cloud_conn_id=TEST_GCP_CONN_ID,
                polling_interval=5.0,
            )


class TestGCSUploadSessionCompleteSensorAsync:
    OPERATOR = GCSUploadSessionCompleteSensorAsync(
        task_id="gcs-obj-session",
        bucket=TEST_BUCKET,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        prefix=TEST_OBJECT,
        inactivity_period=TEST_INACTIVITY_PERIOD,
        min_objects=TEST_MIN_OBJECTS,
    )

    def test_gcs_upload_session_complete_sensor_async(self, context):
        """
        Asserts that a task is deferred and a GCSUploadSessionTrigger will be fired
        when the GCSUploadSessionCompleteSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(context)
        assert isinstance(
            exc.value.trigger, GCSUploadSessionTrigger
        ), "Trigger is not a GCSUploadSessionTrigger"

    def test_gcs_upload_session_complete_sensor_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_gcs_upload_session_complete_sensor_async_execute_complete(self, context):
        """Asserts that execute complete is completed as expected"""

        assert self.OPERATOR.execute_complete(
            context=context, event={"status": "success", "message": "success"}
        )

    def test_poll_interval_deprecation_warning_gcs_upload_session(self):
        """Test DeprecationWarning for GCSUploadSessionCompleteSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            GCSUploadSessionCompleteSensorAsync(
                task_id="task-id",
                bucket=TEST_BUCKET,
                google_cloud_conn_id=TEST_GCP_CONN_ID,
                prefix=TEST_OBJECT,
                inactivity_period=TEST_INACTIVITY_PERIOD,
                min_objects=TEST_MIN_OBJECTS,
                polling_interval=5.0,
            )


class TestGCSObjectUpdateSensorAsync:
    OPERATOR = GCSObjectUpdateSensorAsync(
        task_id="gcs-obj-update",
        bucket=TEST_BUCKET,
        object=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )

    @mock.patch(f"{MODULE}.GCSObjectUpdateSensorAsync.defer")
    @mock.patch(f"{MODULE}.GCSObjectUpdateSensorAsync.poke", return_value=True)
    def test_gcs_object_update_sensor_async_finish_before_deferred(self, mock_poke, mock_defer, context):
        """Assert task is not deferred when it receives a finish status before deferring"""
        self.OPERATOR.execute(create_context(self.OPERATOR))
        assert not mock_defer.called

    @mock.patch(f"{MODULE}.GCSObjectUpdateSensorAsync.poke", return_value=False)
    def test_gcs_object_update_sensor_async(self, context):
        """
        Asserts that a task is deferred and a GCSBlobTrigger will be fired
        when the GCSObjectUpdateSensorAsync is executed.
        """

        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(create_context(self.OPERATOR))
        assert isinstance(
            exc.value.trigger, GCSCheckBlobUpdateTimeTrigger
        ), "Trigger is not a GCSCheckBlobUpdateTimeTrigger"

    def test_gcs_object_update_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""

        with pytest.raises(AirflowException):
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "message": "test failure message"}
            )

    def test_gcs_object_update_sensor_async_execute_complete(self, context):
        """Asserts that logging occurs as expected"""

        with mock.patch.object(self.OPERATOR.log, "info") as mock_log_info:
            self.OPERATOR.execute_complete(
                context=context, event={"status": "success", "message": "Job completed"}
            )
        mock_log_info.assert_called_with(
            "Sensor checks update time for object %s in bucket : %s", TEST_OBJECT, TEST_BUCKET
        )

    def test_poll_interval_deprecation_warning(self):
        """Test DeprecationWarning for GCSObjectUpdateSensorAsync by setting param poll_interval"""
        # TODO: Remove once deprecated
        with pytest.warns(expected_warning=DeprecationWarning):
            GCSObjectUpdateSensorAsync(
                task_id="task-id",
                bucket=TEST_BUCKET,
                object=TEST_OBJECT,
                google_cloud_conn_id=TEST_GCP_CONN_ID,
                polling_interval=5.0,
            )
