#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensorAsync,
    GCSObjectsWithPrefixExistenceSensorAsync,
    GCSUploadSessionCompleteSensorAsync,
)
from astronomer.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSPrefixBlobTrigger,
    GCSUploadSessionTrigger,
)

TEST_BUCKET = "TEST_BUCKET"
TEST_OBJECT = "TEST_OBJECT"
TEST_GCP_CONN_ID = "TEST_GCP_CONN_ID"
TEST_DAG_ID = "unit_tests_gcs_sensor"
TEST_INACTIVITY_PERIOD = 5
TEST_MIN_OBJECTS = 1


@pytest.fixture(scope="function")
def context():
    """
    Creates an empty context.
    """
    yield


def test_gcs_object_existence_sensor_async():
    """
    Asserts that a task is deferred and a GCSBlobTrigger will be fired
    when the GCSObjectExistenceSensorAsync is executed.
    """
    task = GCSObjectExistenceSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        object=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, GCSBlobTrigger), "Trigger is not a GCSBlobTrigger"


def test_gcs_object_existence_sensor_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = GCSObjectExistenceSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        object=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_gcs_object_existence_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = GCSObjectExistenceSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        object=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(context=None, event={"status": "success", "message": "Job completed"})
    mock_log_info.assert_called_with("File %s was found in bucket %s.", TEST_OBJECT, TEST_BUCKET)


def test_gcs_object_with_prefix_existence_sensor_async():
    """
    Asserts that a task is deferred and a GCSPrefixBlobTrigger will be fired
    when the GCSObjectsWithPrefixExistenceSensorAsync is executed.
    """
    task = GCSObjectsWithPrefixExistenceSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        prefix=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, GCSPrefixBlobTrigger), "Trigger is not a GCSPrefixBlobTrigger"


def test_gcs_object_with_prefix_existence_sensor_async_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = GCSObjectsWithPrefixExistenceSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        prefix=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_gcs_object_with_prefix_existence_sensor_async_execute_complete():
    """Asserts that logging occurs as expected"""
    task = GCSObjectsWithPrefixExistenceSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        prefix=TEST_OBJECT,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
    )
    with mock.patch.object(task.log, "info") as mock_log_info:
        task.execute_complete(
            context=None, event={"status": "success", "message": "Job completed", "matches": [TEST_OBJECT]}
        )
    mock_log_info.assert_called_with("Sensor checks existence of objects: %s, %s", TEST_BUCKET, TEST_OBJECT)


def test_gcs_upload_session_complete_sensor_async():
    """
    Asserts that a task is deferred and a GCSUploadSessionTrigger will be fired
    when the GCSUploadSessionCompleteSensorAsync is executed.
    """
    task = GCSUploadSessionCompleteSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        prefix=TEST_OBJECT,
        inactivity_period=TEST_INACTIVITY_PERIOD,
        min_objects=TEST_MIN_OBJECTS,
    )
    with pytest.raises(TaskDeferred) as exc:
        task.execute(context)
    assert isinstance(exc.value.trigger, GCSUploadSessionTrigger), "Trigger is not a GCSUploadSessionTrigger"


def test_gcs_upload_session_complete_sensor_execute_failure(context):
    """Tests that an AirflowException is raised in case of error event"""
    task = GCSUploadSessionCompleteSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        prefix=TEST_OBJECT,
        inactivity_period=TEST_INACTIVITY_PERIOD,
        min_objects=TEST_MIN_OBJECTS,
    )
    with pytest.raises(AirflowException):
        task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})


def test_gcs_upload_session_complete_sensor_async_execute_complete():
    """Asserts that execute complete is completed as expected"""
    task = GCSUploadSessionCompleteSensorAsync(
        task_id="task-id",
        bucket=TEST_BUCKET,
        google_cloud_conn_id=TEST_GCP_CONN_ID,
        prefix=TEST_OBJECT,
        inactivity_period=TEST_INACTIVITY_PERIOD,
        min_objects=TEST_MIN_OBJECTS,
    )
    assert task.execute_complete(context=None, event={"status": "success", "message": "success"})
