from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
    GCSObjectsWithPrefixExistenceSensor,
    GCSObjectUpdateSensor,
    GCSUploadSessionCompleteSensor,
)

from astronomer.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensorAsync,
    GCSObjectsWithPrefixExistenceSensorAsync,
    GCSObjectUpdateSensorAsync,
    GCSUploadSessionCompleteSensorAsync,
)

TEST_BUCKET = "TEST_BUCKET"
TEST_OBJECT = "TEST_OBJECT"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_INACTIVITY_PERIOD = 5
TEST_MIN_OBJECTS = 1

MODULE = "astronomer.providers.google.cloud.sensors.gcs"


class TestGCSObjectExistenceSensorAsync:
    def test_init(self):
        task = GCSObjectExistenceSensorAsync(
            task_id="gcs-object",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
        )

        assert isinstance(task, GCSObjectExistenceSensor)
        assert task.deferrable is True


class TestGCSObjectsWithPrefixExistenceSensorAsync:
    def test_init(self):
        task = GCSObjectsWithPrefixExistenceSensorAsync(
            task_id="gcs-obj-prefix",
            bucket=TEST_BUCKET,
            prefix=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
        )

        assert isinstance(task, GCSObjectsWithPrefixExistenceSensor)
        assert task.deferrable is True


class TestGCSUploadSessionCompleteSensorAsync:
    def test_init(self):
        task = GCSUploadSessionCompleteSensorAsync(
            task_id="gcs-obj-session",
            bucket=TEST_BUCKET,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
            prefix=TEST_OBJECT,
            inactivity_period=TEST_INACTIVITY_PERIOD,
            min_objects=TEST_MIN_OBJECTS,
        )

        assert isinstance(task, GCSUploadSessionCompleteSensor)
        assert task.deferrable is True


class TestGCSObjectUpdateSensorAsync:
    def test_init(self):
        task = GCSObjectUpdateSensorAsync(
            task_id="gcs-obj-update",
            bucket=TEST_BUCKET,
            object=TEST_OBJECT,
            google_cloud_conn_id=TEST_GCP_CONN_ID,
        )

        assert isinstance(task, GCSObjectUpdateSensor)
        assert task.deferrable is True
