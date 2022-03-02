from datetime import datetime, timedelta

from airflow.models.dag import DAG

from astronomer.providers.amazon.aws.sensors.s3 import (
    S3KeySensorAsync,
    S3KeySizeSensorAsync,
)

default_args = {
    "retry": 5,
    "retry_delay": timedelta(minutes=1),
}

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
        bucket_key="sample_key.txt",
        wildcard_match=False,
        bucket_name="sample-bucket",
    )

    check_if_wildcard_exists = S3KeySensorAsync(
        task_id="check_if_wildcard_exists",
        bucket_key="test*",
        wildcard_match=True,
        bucket_name="sample-bucket",
    )

    check_if_key_with_size_without_wildcard = S3KeySizeSensorAsync(
        task_id="check_if_key_with_size_without_wildcard",
        bucket_key="sample_key.txt",
        wildcard_match=False,
        bucket_name="test-team-providers-ankit",
    )

    check_if_key_with_size_with_wildcard = S3KeySizeSensorAsync(
        task_id="check_if_key_with_size_with_wildcard",
        bucket_key="sample*",
        wildcard_match=True,
        bucket_name="test-team-providers-ankit",
    )

    waiting_for_s3_key >> check_if_wildcard_exists >> check_if_key_with_size_without_wildcard
    check_if_key_with_size_without_wildcard >> check_if_key_with_size_with_wildcard
