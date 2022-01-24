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
"""
Example Airflow DAG for Google Cloud Storage operators.
"""

import os
from datetime import datetime
from tempfile import gettempdir

from airflow import models

from astronomer_operators.google.cloud.sensors.gcs import GCSAsyncObjectExistenceSensor
# from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

START_DATE = datetime(2022, 1, 1)

CONNECTION_ID = os.environ.get("CONNECTION_ID","my_connection")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "astronomer-airflow-providers ")
BUCKET_1 = os.environ.get("GCP_GCS_BUCKET_1", "test_bucket_rajath")
GCS_ACL_ENTITY = os.environ.get("GCS_ACL_ENTITY", "allUsers")
GCS_ACL_BUCKET_ROLE = "OWNER"
GCS_ACL_OBJECT_ROLE = "OWNER"

BUCKET_2 = os.environ.get("GCP_GCS_BUCKET_2", "test-gcs-example-bucket-2")

temp_dir_path = gettempdir()
PATH_TO_TRANSFORM_SCRIPT = os.environ.get(
    "GCP_GCS_PATH_TO_TRANSFORM_SCRIPT", os.path.join(temp_dir_path, "transform_script.py")
)
PATH_TO_UPLOAD_FILE = os.environ.get(
    "GCP_GCS_PATH_TO_UPLOAD_FILE", os.path.join(temp_dir_path, "test-gcs-example-upload.txt")
)
PATH_TO_UPLOAD_FILE_PREFIX = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE_PREFIX", "test-gcs-")
PATH_TO_SAVED_FILE = os.environ.get(
    "GCP_GCS_PATH_TO_SAVED_FILE", os.path.join(temp_dir_path, "test-gcs-example-download.txt")
)

# BUCKET_FILE_LOCATION = PATH_TO_UPLOAD_FILE.rpartition("/")[-1]
BUCKET_FILE_LOCATION =  "wrong.txt"



with models.DAG(
    "example_async_gcs_sensors",
    start_date=START_DATE,
    catchup=False,
    schedule_interval='@once',
    tags=['example'],
) as dag:
    # [START howto_sensor_object_exists_task]
    gcs_object_exists = GCSAsyncObjectExistenceSensor(
        bucket=BUCKET_1,
        object=BUCKET_FILE_LOCATION,
        mode='poke',
        task_id="gcs_object_exists_task",
        google_cloud_conn_id=CONNECTION_ID,
    )
    # [END howto_sensor_object_exists_task]
    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_exists


if __name__ == '__main__':
    dag.run()