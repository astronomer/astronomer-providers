"""DAG to nuke AWS resources."""

import logging
import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "**********")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_NUKE_DAG_SCHEDULE = os.getenv("AWS_NUKE_DAG_SCHEDULE", None)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "***********")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#provider-alert")
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow_app")
SLACK_WEBHOOK_CONN = os.getenv("SLACK_WEBHOOK_CONN", "http_slack")

REGRESSION_CLUSTER_AWS_ACCESS_KEY = os.getenv("REGRESSION_CLUSTER_AWS_ACCESS_KEY", "**********")
REGRESSION_CLUSTER_AWS_SECRET_ACCESS_KEY = os.getenv(
    "REGRESSION_CLUSTER_AWS_SECRET_ACCESS_KEY", "***********"
)
REGRESSION_CLUSTER_AWS_DEFAULT_REGION = os.getenv("REGRESSION_CLUSTER_AWS_DEFAULT_REGION", "us-east-1")


def generate_task_report(**context: Any) -> None:
    """Generate a report of the task statuses for the DAG run and send it to configured Slack channel for alerts."""
    dag_run = context["dag_run"]
    run_id = dag_run.run_id

    report = f"*Report for `AWS nuke` DAG run ID: `{run_id}`*\n\n"

    airflow_version = context["ti"].xcom_pull(task_ids="get_airflow_version")
    report += f"*Airflow version*: `{airflow_version}`\n"
    airflow_executor = context["ti"].xcom_pull(task_ids="get_airflow_executor")
    report += f"*Airflow executor*: `{airflow_executor}`\n\n"

    dag = context["dag"]
    ordered_task_ids = [task.task_id for task in dag.topological_sort()]

    # Retrieve task instances for the DagRun
    task_instances = dag_run.get_task_instances()

    # Sort the task instances based on the ordered task IDs
    ordered_task_instances = sorted(task_instances, key=lambda ti: ordered_task_ids.index(ti.task_id))

    # Iterate over each task instance and append its status to the report
    for task_instance in ordered_task_instances:
        task_id = task_instance.task_id
        if task_id in (
            "start",
            "get_airflow_version",
            "get_airflow_executor",
            "generate_report",
            "dag_final_status",
        ):
            continue
        task_status = task_instance.current_state()
        task_status_icon = ":black_circle:"
        if task_status == State.SUCCESS:
            task_status_icon = ":large_green_circle:"
        elif task_status == State.FAILED:
            task_status_icon = ":red_circle:"
        elif task_status == State.UPSTREAM_FAILED:
            task_status_icon = ":large_orange_circle:"
        report += f"{task_status_icon} {task_id} - {task_status} \n"

    try:
        # Send the report as a Slack message
        SlackWebhookOperator(
            task_id="send_slack_report",
            slack_webhook_conn_id=SLACK_WEBHOOK_CONN,
            message=report,
            channel=SLACK_CHANNEL,
            username=SLACK_USERNAME,
        ).execute(context={})
    except Exception as exception:
        logging.exception("Error occur while sending slack alert.")
        raise exception


def check_dag_status(**kwargs: Any) -> None:
    """Raise an exception if any of the DAG's tasks failed and as a result marking the DAG failed."""
    for task_instance in kwargs["dag_run"].get_task_instances():
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_aws_nuke",
    start_date=datetime(2022, 1, 1),
    schedule=AWS_NUKE_DAG_SCHEDULE,
    catchup=False,
    default_args=default_args,
    tags=["example", "aws-nuke"],
    is_paused_upon_creation=False,
) as dag:
    start = EmptyOperator(task_id="start")

    get_airflow_version = BashOperator(
        task_id="get_airflow_version", bash_command="airflow version", do_xcom_push=True
    )

    get_airflow_executor = BashOperator(
        task_id="get_airflow_executor",
        bash_command="airflow config get-value core executor",
        do_xcom_push=True,
    )

    terminate_running_emr_virtual_clusters = BashOperator(
        task_id="terminate_running_emr_virtual_clusters",
        bash_command=f"set -e; "
        f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; "
        f"aws emr-containers list-virtual-clusters --state RUNNING --region {AWS_DEFAULT_REGION} | jq -r '.virtualClusters[].id' | xargs -I % aws emr-containers delete-virtual-cluster --id % --region {AWS_DEFAULT_REGION}; ",
    )

    terminate_dag_authoring_regression_clusters = BashOperator(
        task_id="terminate_dag_authoring_regression_clusters",
        bash_command=f"set -e; "
        f"aws configure set aws_access_key_id {REGRESSION_CLUSTER_AWS_ACCESS_KEY}; "
        f"aws configure set aws_secret_access_key {REGRESSION_CLUSTER_AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {REGRESSION_CLUSTER_AWS_DEFAULT_REGION}; "
        f"sh $AIRFLOW_HOME/dags/example_delete_eks_cluster_and_nodes.sh {REGRESSION_CLUSTER_AWS_DEFAULT_REGION}",
    )

    execute_aws_nuke = BashOperator(
        task_id="execute_aws_nuke",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; "
        f"aws-nuke -c /usr/local/airflow/dags/nuke-config.yml --profile default --force --no-dry-run; ",
    )

    delete_stale_emr_vpcs = BashOperator(
        task_id="delete_stale_emr_vpcs",
        bash_command="sh $AIRFLOW_HOME/dags/example_delete_stale_emr_vpcs.sh ",
        trigger_rule="all_done",
    )

    delete_stale_emr_iam_roles = BashOperator(
        task_id="delete_stale_emr_iam_roles",
        bash_command="sh $AIRFLOW_HOME/dags/example_delete_stale_emr_iam_roles.sh ",
    )

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_task_report,
        provide_context=True,
        trigger_rule="all_done",
    )

    dag_final_status = PythonOperator(
        task_id="dag_final_status",
        provide_context=True,
        python_callable=check_dag_status,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        retries=0,
    )

    (
        start
        >> [get_airflow_version, get_airflow_executor]
        >> terminate_running_emr_virtual_clusters
        >> terminate_dag_authoring_regression_clusters
        >> execute_aws_nuke
        >> delete_stale_emr_vpcs
        >> delete_stale_emr_iam_roles
        >> generate_report
        >> dag_final_status
    )
