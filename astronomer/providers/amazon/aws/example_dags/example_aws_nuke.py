"""DAG to nuke AWS resources."""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "**********")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "***********")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_aws_nuke",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "aws-nuke"],
    is_paused_upon_creation=False,
) as dag:
    start = DummyOperator(task_id="start")

    terminate_running_emr_virtual_clusters = BashOperator(
        task_id="terminate_running_emr_virtual_clusters",
        bash_command=f"set -e; "
        f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; "
        f"aws emr-containers list-virtual-clusters --state RUNNING --region {AWS_DEFAULT_REGION} | jq -r '.virtualClusters[].id' | xargs -I % aws emr-containers delete-virtual-cluster --id % --region {AWS_DEFAULT_REGION}; ",
    )

    execute_aws_nuke = BashOperator(
        task_id="execute_aws_nuke",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; "
        f"aws-nuke -c /usr/local/airflow/dags/nuke-config.yml --profile default --force --no-dry-run; ",
    )

    end = DummyOperator(task_id="end")

    start >> terminate_running_emr_virtual_clusters >> execute_aws_nuke >> end
