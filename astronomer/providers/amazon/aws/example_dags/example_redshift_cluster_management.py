import logging
import os
import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)
from astronomer.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)

REDSHIFT_CLUSTER_IDENTIFIER = os.getenv("REDSHIFT_CLUSTER_IDENTIFIER", "astro-providers-cluster")
REDSHIFT_CLUSTER_MASTER_USER = os.getenv("REDSHIFT_CLUSTER_MASTER_USER", "awsuser")
REDSHIFT_CLUSTER_MASTER_PASSWORD = os.getenv("REDSHIFT_CLUSTER_MASTER_PASSWORD", "********")
REDSHIFT_CLUSTER_DB_NAME = os.getenv("REDSHIFT_CLUSTER_DB_NAME", "astro_dev")
REDSHIFT_CLUSTER_TYPE = os.getenv("REDSHIFT_CLUSTER_TYPE", "single-node")
REDSHIFT_CLUSTER_NODE_TYPE = os.getenv("REDSHIFT_CLUSTER_NODE_TYPE", "dc2.large")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "**********")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "***********")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}


def get_cluster_status() -> str:
    """Get the status of aws redshift cluster"""
    client = boto3.client("redshift")

    response = client.describe_clusters(
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
    )
    logging.info("%s", response)
    cluster = response.get("Clusters")[0]
    cluster_status: str = cluster.get("ClusterStatus")
    return cluster_status


def get_snapshot_status() -> str:
    """Get the status of aws redshift cluster snapshot"""
    client = boto3.client("redshift")

    response = client.describe_cluster_snapshots(
        SnapshotIdentifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
    )
    logging.info("%s", response)
    snapshot = response.get("Snapshots")[0]
    snapshot_status: str = snapshot.get("Status")
    return snapshot_status


def create_redshift_cluster() -> None:
    """Create aws redshift cluster and wait until it available"""
    client = boto3.client("redshift")

    client.create_cluster(
        DBName=REDSHIFT_CLUSTER_DB_NAME,
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        ClusterType=REDSHIFT_CLUSTER_TYPE,
        NodeType=REDSHIFT_CLUSTER_NODE_TYPE,
        MasterUsername=REDSHIFT_CLUSTER_MASTER_USER,
        MasterUserPassword=REDSHIFT_CLUSTER_MASTER_PASSWORD,
        Tags=[
            {"Key": "Purpose", "Value": "ProviderTest"},
        ],
    )

    while True:
        if get_cluster_status() == "available":
            break
        time.sleep(30)


def create_redshift_cluster_snapshot() -> None:
    """Create aws redshift cluster snapshot and wait until it available"""
    client = boto3.client("redshift")
    client.create_cluster_snapshot(
        SnapshotIdentifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        Tags=[
            {"Key": "Purpose", "Value": "ProviderTest"},
        ],
    )

    while True:
        if get_snapshot_status() == "available":
            break
        time.sleep(30)
    # Sometime cluster take few seconds to goes to modifying state again
    time.sleep(60)
    while True:
        if get_cluster_status() == "available":
            break
        time.sleep(30)


def delete_redshift_cluster_snapshot() -> None:
    """Delete a aws redshift cluster and wait until cluster is available"""
    client = boto3.client("redshift")

    try:
        client.delete_cluster_snapshot(
            SnapshotIdentifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
            SnapshotClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        )

        while True:
            if get_snapshot_status() == "deleting":
                time.sleep(30)
                continue

        while True:
            if get_cluster_status() == "available":
                time.sleep(30)
                continue
    except ClientError:
        logging.exception("Error when deleting the cluster")
        return None


def delete_redshift_cluster() -> None:
    """Delete a redshift cluster and wait until it completely removed"""
    client = boto3.client("redshift")

    try:
        client.delete_cluster(
            ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True,
        )

        while True:
            if get_cluster_status() == "deleting":
                time.sleep(30)
                continue
    except ClientError:
        logging.exception("Error when deleting the cluster")
        return None


with DAG(
    dag_id="example_async_redshift_cluster_management",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "redshift"],
) as dag:
    start = DummyOperator(task_id="start")

    config = BashOperator(
        task_id="aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    create_cluster_op = PythonOperator(
        task_id="create_redshift_cluster",
        python_callable=create_redshift_cluster,
    )

    create_cluster_snapshot_op = PythonOperator(
        task_id="create_cluster_snapshot",
        python_callable=create_redshift_cluster_snapshot,
    )

    # [START howto_operator_redshift_pause_cluster_async]
    pause_cluster_task = RedshiftPauseClusterOperatorAsync(
        task_id="pause_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_redshift_pause_cluster_async]

    # [START howto_operator_redshift_resume_cluster_async]
    resume_cluster_task = RedshiftResumeClusterOperatorAsync(
        task_id="resume_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_redshift_resume_cluster_async]

    # [START howto_operator_redshift_cluster_sensor_async]
    async_redshift_sensor_task = RedshiftClusterSensorAsync(
        task_id="redshift_sensor",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        target_status="available",
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_redshift_cluster_sensor_async]

    delete_cluster_snapshot_op = PythonOperator(
        task_id="delete_redshift_cluster_snapshot",
        python_callable=delete_redshift_cluster_snapshot,
        trigger_rule="all_done",
    )

    delete_cluster_op = PythonOperator(
        task_id="delete_redshift_cluster",
        python_callable=delete_redshift_cluster,
        trigger_rule="all_done",
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> config
        >> create_cluster_op
        >> create_cluster_snapshot_op
        >> pause_cluster_task
        >> [resume_cluster_task, async_redshift_sensor_task]
        >> delete_cluster_snapshot_op
        >> delete_cluster_op
    )

    [resume_cluster_task, async_redshift_sensor_task] >> end
    [delete_cluster_snapshot_op, delete_cluster_op] >> end
