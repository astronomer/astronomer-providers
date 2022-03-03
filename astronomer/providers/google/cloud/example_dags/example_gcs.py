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

from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

from astronomer.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensorAsync,
    GCSObjectsWithPrefixExistenceSensorAsync,
)

START_DATE = datetime(2021, 1, 1)

PROJECT_ID = "astronomer-airflow-providers"
BUCKET_1 = "test-gcs-example-bucket"
PATH_TO_UPLOAD_FILE = "dags/example_gcs.py"
PATH_TO_UPLOAD_FILE_PREFIX = "exmaple_"

BUCKET_FILE_LOCATION = "example_gcs.py"

with models.DAG(
    "example_async_gcs_sensors",
    start_date=START_DATE,
    catchup=False,
    schedule_interval='@once',
    tags=['example'],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_1, project_id=PROJECT_ID
    )
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
    )
    # [START howto_sensor_object_exists_task]
    gcs_object_exists = GCSObjectExistenceSensorAsync(
        bucket=BUCKET_1,
        object=BUCKET_FILE_LOCATION,
        task_id="gcs_object_exists_task_async",
    )
    # [END howto_sensor_object_exists_task]
    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensorAsync(
        bucket=BUCKET_1,
        prefix=PATH_TO_UPLOAD_FILE_PREFIX,
        task_id="gcs_object_with_prefix_exists_task_async",
    )
    # [END howto_sensor_object_with_prefix_exists_task]
    delete_bucket = GCSDeleteBucketOperator(task_id="delete_bucket", bucket_name=BUCKET_1)

    create_bucket >> upload_file >> [gcs_object_exists, gcs_object_with_prefix_exists] >> delete_bucket


if __name__ == '__main__':
    dag.clear()
    dag.run()
