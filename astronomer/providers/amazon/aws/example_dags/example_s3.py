import os
from datetime import datetime, timedelta

from airflow import DAG

from astronomer.providers.amazon.aws.sensors.s3 import (
    S3KeySensorAsync,
    S3KeySizeSensorAsync,
    S3KeysUnchangedSensorAsync,
)

default_args = {
    "retry": 5,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "astronomer-airflow-providers")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "test-team-providers")
S3_BUCKET_KEY = os.environ.get("S3_BUCKET_KEY", "sample_key.txt")
S3_BUCKET_WILDCARD_KEY = os.environ.get("S3_BUCKET_WILDCARD_KEY", "test*")
WILDCARD_KEY_FALSE = os.environ.get("WILDCARD_KEY_FALSE", False)
WILDCARD_KEY_TRUE = os.environ.get("WILDCARD_KEY_TRUE", True)
PREFIX = os.environ.get("PREFIX", "test")
INACTIVITY_PERIOD = os.environ.get("INACTIVITY_PERIOD", 5)

with DAG(
    dag_id="example_s3_key_sensor",
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["async"],
) as dag:

    waiting_for_s3_key = S3KeySensorAsync(
        task_id="waiting_for_s3_key",
        bucket_key=S3_BUCKET_KEY,
        wildcard_match=WILDCARD_KEY_FALSE,
        bucket_name=S3_BUCKET_NAME,
    )

    check_if_wildcard_exists = S3KeySensorAsync(
        task_id="check_if_wildcard_exists",
        bucket_key=S3_BUCKET_WILDCARD_KEY,
        wildcard_match=WILDCARD_KEY_TRUE,
        bucket_name=S3_BUCKET_NAME,
    )

    check_if_key_with_size_without_wildcard = S3KeySizeSensorAsync(
        task_id="check_if_key_with_size_without_wildcard",
        bucket_key=S3_BUCKET_KEY,
        wildcard_match=WILDCARD_KEY_FALSE,
        bucket_name=S3_BUCKET_NAME,
    )

    check_if_key_with_size_with_wildcard = S3KeySizeSensorAsync(
        task_id="check_if_key_with_size_with_wildcard",
        bucket_key=S3_BUCKET_WILDCARD_KEY,
        wildcard_match=WILDCARD_KEY_TRUE,
        bucket_name=S3_BUCKET_NAME,
    )

    check_s3_key_unchanged_sensor = S3KeysUnchangedSensorAsync(
        task_id="check_s3_key_unchanged_sensor",
        bucket_name=S3_BUCKET_NAME,
        prefix=PREFIX,
        inactivity_period=INACTIVITY_PERIOD,
    )

(
    waiting_for_s3_key
    >> check_if_wildcard_exists
    >> check_if_key_with_size_without_wildcard
    >> check_if_key_with_size_with_wildcard
    >> check_s3_key_unchanged_sensor
)
