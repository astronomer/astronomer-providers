"""Example Airflow DAG for Google Cloud Storage operators."""
import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
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
    GCSObjectUpdateSensorAsync,
    GCSUploadSessionCompleteSensorAsync,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "astronomer-airflow-providers")
BUCKET_1 = os.environ.get("GCP_TEST_BUCKET", "test-gcs-bucket-astronomer-providers")
GCP_CONN_ID = os.environ.get("GCP_CONN_ID", "google_cloud_default")
PATH_TO_UPLOAD_FILE = "dags/example_gcs.py"
PATH_TO_UPLOAD_FILE_PREFIX = "example_"
BUCKET_FILE_LOCATION = "example_gcs.py"


with DAG(
    "example_async_gcs_sensors",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "async", "gcs"],
) as dag:
    # [START howto_create_bucket_task]
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket1",
        bucket_name=BUCKET_1,
        project_id=PROJECT_ID,
        resource={
            "iamConfiguration": {
                "uniformBucketLevelAccess": {
                    "enabled": False,
                },
            },
        },
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_create_bucket_task]
    # [START delay_bash_operator_task]
    delay_bash_operator_task = BashOperator(task_id="delay_bash_operator_task", bash_command="sleep 15s")
    # [END delay_bash_operator_task]
    # [START howto_upload_file_task]
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=PATH_TO_UPLOAD_FILE,
        dst=BUCKET_FILE_LOCATION,
        bucket=BUCKET_1,
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_upload_file_task]
    # [START howto_sensor_object_exists_task]
    gcs_object_exists = GCSObjectExistenceSensorAsync(
        bucket=BUCKET_1,
        object=BUCKET_FILE_LOCATION,
        task_id="gcs_object_exists_task",
        execution_timeout=timedelta(seconds=60),
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_sensor_object_exists_task]
    # [START howto_sensor_object_with_prefix_exists_task]
    gcs_object_with_prefix_exists = GCSObjectsWithPrefixExistenceSensorAsync(
        bucket=BUCKET_1,
        prefix=PATH_TO_UPLOAD_FILE_PREFIX,
        task_id="gcs_object_with_prefix_exists_task",
        execution_timeout=timedelta(seconds=60),
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_sensor_object_with_prefix_exists_task]
    # [START howto_sensor_gcs_upload_session_complete_task]
    gcs_upload_session_complete = GCSUploadSessionCompleteSensorAsync(
        bucket=BUCKET_1,
        prefix=PATH_TO_UPLOAD_FILE_PREFIX,
        inactivity_period=60,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        task_id="gcs_upload_session_complete_task",
        execution_timeout=timedelta(seconds=60),
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_sensor_gcs_upload_session_complete_task]
    # [START howto_sensor_object_update_exists_task]
    gcs_update_object_exists = GCSObjectUpdateSensorAsync(
        bucket=BUCKET_1,
        object=BUCKET_FILE_LOCATION,
        task_id="gcs_object_update_sensor_task_async",
        execution_timeout=timedelta(seconds=60),
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_sensor_object_update_exists_task]
    # [START howto_delete_buckettask]
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_1,
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_delete_buckettask]
    (
        create_bucket
        >> [
            delay_bash_operator_task,
            gcs_object_exists,
            gcs_object_with_prefix_exists,
            gcs_upload_session_complete,
        ]
    )
    (
        delay_bash_operator_task
        >> upload_file
        >> gcs_update_object_exists
        >> delete_bucket
        << [
            gcs_object_exists,
            gcs_object_with_prefix_exists,
            gcs_upload_session_complete,
            gcs_update_object_exists,
        ]
    )

if __name__ == "__main__":
    dag.clear()
    dag.run()
