import os
from datetime import datetime, timedelta
from typing import Any, List

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

from astronomer.providers.amazon.aws.sensors.s3 import (
    S3KeySensorAsync,
    S3KeysUnchangedSensorAsync,
)

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "test-astronomer-providers-bucket")
S3_BUCKET_KEY = os.getenv("S3_BUCKET_KEY", "test")
S3_BUCKET_KEY_LIST = os.getenv("S3_BUCKET_KEY_LIST", "test2")
S3_BUCKET_WILDCARD_KEY = os.getenv("S3_BUCKET_WILDCARD_KEY", "test*")
PREFIX = os.getenv("S3_PREFIX", "test")
INACTIVITY_PERIOD = float(os.getenv("INACTIVITY_PERIOD", 5))
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
LOCAL_FILE_PATH = os.getenv("LOCAL_FILE_PATH", "/usr/local/airflow/dags/example_s3_test_file.txt")
AWS_CONN_ID = os.getenv("ASTRO_AWS_S3_CONN_ID", "aws_s3_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
DATA = os.environ.get(
    "DATA",
    """
apple,0.5
milk,2.5
bread,4.0
""",
)

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def check_dag_status(**kwargs: Any) -> None:
    """Raises an exception if any of the DAG's tasks failed and as a result marking the DAG failed."""
    for task_instance in kwargs["dag_run"].get_task_instances():
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")


with DAG(
    dag_id="example_s3_sensor",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "s3"],
) as dag:
    # [START howto_sensor_s3_key_function_definition]
    def check_fn(files: List[Any]) -> bool:
        """
        Check if all files are bigger than 0 bytes

        :param files: List of S3 object attributes.
        :return: true if the criteria is met
        """
        return all(f.get("Size", 0) > 0 for f in files)

    # [END howto_sensor_s3_key_function_definition]
    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        region_name=AWS_DEFAULT_REGION,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    create_object = S3CreateObjectOperator(
        task_id="s3_create_object",
        s3_bucket=S3_BUCKET_NAME,
        s3_key=S3_BUCKET_KEY,
        data=DATA,
        replace=True,
    )

    # [START howto_sensor_async_s3_key_single_key]
    # Check if a file exists
    sensor_one_key = S3KeySensorAsync(
        task_id="s3_sensor_one_key",
        bucket_name=S3_BUCKET_NAME,
        bucket_key=S3_BUCKET_KEY,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_async_s3_key_single_key]

    create_object_for_key2 = S3CreateObjectOperator(
        task_id="s3_create_object_for_key2",
        s3_bucket=S3_BUCKET_NAME,
        s3_key=S3_BUCKET_KEY_LIST,
        data=DATA,
        replace=True,
    )

    # [START howto_sensor_async_s3_key_multiple_keys]
    # Check if both files exist
    sensor_two_keys = S3KeySensorAsync(
        task_id="s3_sensor_two_keys",
        bucket_name=S3_BUCKET_NAME,
        bucket_key=[S3_BUCKET_KEY, S3_BUCKET_KEY_LIST],
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_async_s3_key_multiple_keys]

    # [START howto_sensor_async_s3_key_function]
    # Check if a file exists and match a certain pattern defined in check_fn
    sensor_key_with_function = S3KeySensorAsync(
        task_id="s3_sensor_key_function",
        bucket_name=S3_BUCKET_NAME,
        bucket_key=S3_BUCKET_KEY,
        check_fn=check_fn,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_async_s3_key_function]

    # [START howto_sensor_s3_key_unchanged_async]
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
    # [END howto_sensor_s3_key_unchanged_async]

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        trigger_rule="all_done",
        force_delete=True,
        bucket_name=S3_BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
    )

    dag_final_status = PythonOperator(
        task_id="dag_final_status",
        provide_context=True,
        python_callable=check_dag_status,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        dag=dag,
        retries=0,
    )

    (
        create_bucket
        >> create_object
        >> [sensor_one_key, create_object_for_key2, sensor_two_keys, sensor_key_with_function]
        >> check_s3_key_unchanged_sensor
        >> delete_bucket
        >> dag_final_status
    )
