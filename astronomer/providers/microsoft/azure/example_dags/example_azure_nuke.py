import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_aws_nuke",
    start_date=datetime(2022, 1, 1),
    schedule_interval="30 20 * * *",
    catchup=False,
    default_args=default_args,
    tags=["example", "aws-nuke"],
    is_paused_upon_creation=False,
) as dag:
    start = DummyOperator(task_id="start")

    set_aws_config = BashOperator(
        task_id="setup_azure_keys",
        bash_command=f"export AZURE_CLIENT_ID={CLIENT_ID}; " f"export AZURE_CLIENT_ID={CLIENT_SECRET}; ",
    )

    execute_aws_nuke = BashOperator(
        task_id="execute_aws_nuke",
        bash_command="azure-nuke nuke /usr/local/airflow/dags/azure-nuke-config.yaml --profile default --force --no-dry-run; ",  # noqa: E501
    )

    end = DummyOperator(task_id="end")

    start >> execute_aws_nuke >> end
