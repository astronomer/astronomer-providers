import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

from astronomer.providers.amazon.aws.sensors.s3 import (
    S3KeySensorAsync,
    S3KeySizeSensorAsync,
    S3KeysUnchangedSensorAsync,
    S3PrefixSensorAsync,
)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "test-bucket-astronomer-providers")
S3_BUCKET_KEY = os.getenv("S3_BUCKET_KEY", "test/example_s3_test_file.txt")
S3_BUCKET_WILDCARD_KEY = os.getenv("S3_BUCKET_WILDCARD_KEY", "test*")
PREFIX = os.getenv("S3_PREFIX", "test")
INACTIVITY_PERIOD = float(os.getenv("INACTIVITY_PERIOD", 5))
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
LOCAL_FILE_PATH = os.getenv("LOCAL_FILE_PATH", "/usr/local/airflow/dags/example_s3_test_file.txt")
AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "retry": 5,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}

with DAG(
    dag_id="example_s3_sensor",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "s3"],
) as dag:
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        region_name=AWS_DEFAULT_REGION,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    create_local_to_s3_job = LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename=LOCAL_FILE_PATH,
        dest_key=S3_BUCKET_KEY,
        dest_bucket=S3_BUCKET_NAME,
        replace=True,
        aws_conn_id=AWS_CONN_ID,
    )

    waiting_for_s3_key = S3KeySensorAsync(
        task_id="waiting_for_s3_key",
        bucket_key=S3_BUCKET_KEY,
        wildcard_match=False,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    check_if_wildcard_exists = S3KeySensorAsync(
        task_id="check_if_wildcard_exists",
        bucket_key=S3_BUCKET_WILDCARD_KEY,
        wildcard_match=True,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    check_if_key_with_size_without_wildcard = S3KeySizeSensorAsync(
        task_id="check_if_key_with_size_without_wildcard",
        bucket_key=S3_BUCKET_KEY,
        wildcard_match=False,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    check_if_key_with_size_with_wildcard = S3KeySizeSensorAsync(
        task_id="check_if_key_with_size_with_wildcard",
        bucket_key=S3_BUCKET_WILDCARD_KEY,
        wildcard_match=True,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    check_s3_key_unchanged_sensor = S3KeysUnchangedSensorAsync(
        task_id="check_s3_key_unchanged_sensor",
        bucket_name=S3_BUCKET_NAME,
        prefix=PREFIX,
        min_objects=1,
        allow_delete=True,
        previous_objects=set(),
        inactivity_period=INACTIVITY_PERIOD,
        aws_conn_id=AWS_CONN_ID,
    )

    check_s3_prefix_sensor = S3PrefixSensorAsync(
        task_id="check_s3_prefix_sensor",
        bucket_name=S3_BUCKET_NAME,
        prefix=PREFIX,
        aws_conn_id=AWS_CONN_ID,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        force_delete=True,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

(
    create_bucket
    >> create_local_to_s3_job
    >> waiting_for_s3_key
    >> check_if_wildcard_exists
    >> check_if_key_with_size_without_wildcard
    >> check_if_key_with_size_with_wildcard
    >> check_s3_key_unchanged_sensor
    >> check_s3_prefix_sensor
    >> delete_bucket
)
