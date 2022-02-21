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
Example Airflow DAG for Google Object Existence Sensor.
"""

from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

from astronomer.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensorAsync

START_DATE = datetime(2022, 1, 1)

PATH_TO_UPLOAD_FILE = "dags/example_gcs.py"
CONNECTION_ID = "my_connection"
PROJECT_ID = "astronomer-airflow-providers"
BUCKET_1 = "test_bucket_for_dag"
BUCKET_FILE_LOCATION = "test.txt"


with models.DAG(
    "example_async_gcs_sensors",
    start_date=START_DATE,
    catchup=False,
    schedule_interval="@once",
    tags=["example"],
) as dag:

    create_bucket1 = GCSCreateBucketOperator(
        task_id="create_bucket1",
        bucket_name=BUCKET_1,
        project_id=PROJECT_ID,
        gcp_conn_id=CONNECTION_ID,
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=[PATH_TO_UPLOAD_FILE],
        dst=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
        gcp_conn_id=CONNECTION_ID,
    )

    gcs_object_exists = GCSObjectExistenceSensorAsync(
        bucket=BUCKET_1,
        object=BUCKET_FILE_LOCATION,
        task_id="gcs_task_object_existence_sensor",
        google_cloud_conn_id=CONNECTION_ID,
    )

    delete_bucket_1 = GCSDeleteBucketOperator(
        task_id="delete_bucket_1",
        bucket_name=BUCKET_1,
        gcp_conn_id=CONNECTION_ID,
    )

    create_bucket1 >> upload_file >> gcs_object_exists >> delete_bucket_1


if __name__ == "__main__":
    dag.run()
