from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer_operators.amazon.aws.sensors.s3 import S3KeySensorAsync

default_args = {
    "retry": 5,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="example_s3_key_sensor",
    schedule_interval="@daily",
    start_date=days_ago(3),
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
