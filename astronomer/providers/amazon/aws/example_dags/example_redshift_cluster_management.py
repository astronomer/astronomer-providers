import logging
import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperatorAsync,
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)
from astronomer.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)

AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "**********")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "***********")
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


def get_cluster_status() -> str:
    """Get the status of aws redshift cluster"""
    import boto3

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
    import boto3

    client = boto3.client("redshift")

    response = client.describe_cluster_snapshots(
        SnapshotIdentifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
    )
    logging.info("%s", response)
    snapshot = response.get("Snapshots")[0]
    snapshot_status: str = snapshot.get("Status")
    return snapshot_status


def create_redshift_cluster_callable() -> None:
    """Create aws redshift cluster and wait until it available"""
    import boto3

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

    while get_cluster_status() != "available":
        logging.info("Waiting for cluster to be available. Sleeping for 30 seconds.")
        time.sleep(30)


def create_redshift_cluster_snapshot_callable() -> None:
    """Create aws redshift cluster snapshot and wait until it available"""
    import boto3

    client = boto3.client("redshift")
    client.create_cluster_snapshot(
        SnapshotIdentifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
        ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        Tags=[
            {"Key": "Purpose", "Value": "ProviderTest"},
        ],
    )

    while get_snapshot_status() != "available":
        logging.info("Waiting for cluster snapshot to be available. Sleeping for 30 seconds.")
        time.sleep(30)

    # Introduce sleep to wait for the cluster to be available back from 'modifying' state before proceeding on to the
    # downstream task.
    while get_cluster_status() != "available":
        logging.info("Waiting for cluster to be available. Sleeping for 30 seconds.")
        time.sleep(30)


def delete_redshift_cluster_snapshot_callable() -> None:
    """Delete a aws redshift cluster and wait until cluster is available"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("redshift")

    try:
        client.delete_cluster_snapshot(
            SnapshotIdentifier=f"{REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
            SnapshotClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER,
        )

        while get_snapshot_status() == "deleting":
            logging.info("Waiting for cluster snapshot to be deleted. Sleeping for 30 seconds.")
            time.sleep(30)

    except ClientError as exception:
        if exception.response.get("Error", {}).get("Code", "") == "ClusterSnapshotNotFound":
            logging.error(
                "Snapshot might have already been deleted. Error message is: %s",
                exception.response["Error"]["Message"],
            )
        else:
            logging.exception("Error when deleting the cluster")
            raise

    # Introduce sleep to wait for the cluster to be available back after snapshot activity before proceeding on to
    # the downstream task.
    while get_cluster_status() != "available":
        logging.info("Waiting for cluster to be available. Sleeping for 30 seconds.")
        time.sleep(30)


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

    create_redshift_cluster = PythonOperator(
        task_id="create_redshift_cluster",
        python_callable=create_redshift_cluster_callable,
    )

    create_cluster_snapshot = PythonOperator(
        task_id="create_cluster_snapshot",
        python_callable=create_redshift_cluster_snapshot_callable,
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

    delete_redshift_cluster_snapshot = PythonOperator(
        task_id="delete_redshift_cluster_snapshot",
        python_callable=delete_redshift_cluster_snapshot_callable,
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

    end = DummyOperator(task_id="end")

    (
        start
        >> config
        >> create_redshift_cluster
        >> create_cluster_snapshot
        >> pause_redshift_cluster
        >> [resume_redshift_cluster, redshift_sensor]
        >> delete_redshift_cluster_snapshot
        >> delete_redshift_cluster
    )

    [resume_redshift_cluster, redshift_sensor] >> end
    [delete_redshift_cluster_snapshot, delete_redshift_cluster] >> end
