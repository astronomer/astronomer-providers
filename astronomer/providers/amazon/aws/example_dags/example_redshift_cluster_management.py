"""Airflow operators example to manage AWS Redshift cluster."""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftCreateClusterSnapshotOperator,
    RedshiftDeleteClusterSnapshotOperator,
)

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperatorAsync,
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)
from astronomer.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)

AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
REDSHIFT_CLUSTER_DB_NAME = os.getenv("REDSHIFT_CLUSTER_DB_NAME", "astro_dev")
REDSHIFT_CLUSTER_IDENTIFIER = os.getenv("REDSHIFT_CLUSTER_IDENTIFIER", "astro-providers-cluster")
REDSHIFT_CLUSTER_MASTER_USER = os.getenv("REDSHIFT_CLUSTER_MASTER_USER", "awsuser")
REDSHIFT_CLUSTER_MASTER_PASSWORD = os.getenv("REDSHIFT_CLUSTER_MASTER_PASSWORD", "********")
REDSHIFT_CLUSTER_NODE_TYPE = os.getenv("REDSHIFT_CLUSTER_NODE_TYPE", "dc2.large")
REDSHIFT_CLUSTER_TYPE = os.getenv("REDSHIFT_CLUSTER_TYPE", "single-node")


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


with DAG(
    dag_id="example_async_redshift_cluster_management",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "redshift"],
) as dag:
    start = EmptyOperator(task_id="start")

    create_redshift_cluster = RedshiftCreateClusterOperator(
        task_id="create_redshift_cluster",
        db_name=REDSHIFT_CLUSTER_DB_NAME,
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        cluster_type=REDSHIFT_CLUSTER_TYPE,
        node_type=REDSHIFT_CLUSTER_NODE_TYPE,
        master_username=REDSHIFT_CLUSTER_MASTER_USER,
        master_user_password=REDSHIFT_CLUSTER_MASTER_PASSWORD,
        tags=[
            {"Key": "Purpose", "Value": "ProviderTest"},
        ],
        wait_for_completion=True,
        aws_conn_id=AWS_CONN_ID,
    )

    create_cluster_snapshot = RedshiftCreateClusterSnapshotOperator(
        task_id="create_cluster_snapshot",
        snapshot_identifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        wait_for_completion=True,
        aws_conn_id=AWS_CONN_ID,
    )

    # [START howto_operator_redshift_pause_cluster_async]
    pause_redshift_cluster = RedshiftPauseClusterOperatorAsync(
        task_id="pause_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_redshift_pause_cluster_async]

    # [START howto_operator_redshift_resume_cluster_async]
    resume_redshift_cluster = RedshiftResumeClusterOperatorAsync(
        task_id="resume_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_redshift_resume_cluster_async]

    # [START howto_sensor_redshift_cluster_async]
    redshift_sensor = RedshiftClusterSensorAsync(
        task_id="redshift_sensor",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        target_status="available",
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_redshift_cluster_async]

    delete_redshift_cluster_snapshot = RedshiftDeleteClusterSnapshotOperator(
        task_id="delete_redshift_cluster_snapshot",
        snapshot_identifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        trigger_rule="all_done",
    )

    # [START howto_operator_redshift_delete_cluster_async]
    delete_redshift_cluster = RedshiftDeleteClusterOperatorAsync(
        task_id="delete_redshift_cluster",
        trigger_rule="all_done",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
        skip_final_cluster_snapshot=True,
        final_cluster_snapshot_identifier=None,
    )
    # [END howto_operator_redshift_delete_cluster_async]

    end = EmptyOperator(task_id="end")

    (
        start
        >> create_redshift_cluster
        >> create_cluster_snapshot
        >> pause_redshift_cluster
        >> [resume_redshift_cluster, redshift_sensor]
        >> delete_redshift_cluster_snapshot
        >> delete_redshift_cluster
    )

    [resume_redshift_cluster, redshift_sensor] >> end
    [delete_redshift_cluster_snapshot, delete_redshift_cluster] >> end
