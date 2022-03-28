import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)
from astronomer.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)

REDSHIFT_CLUSTER_IDENTIFIER = os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER", "astro-providers-cluster")
REDSHIFT_CLUSTER_MASTER_USER = os.environ.get("REDSHIFT_CLUSTER_MASTER_USER", "adminuser")
REDSHIFT_CLUSTER_MASTER_PASSWORD = os.environ.get("REDSHIFT_CLUSTER_MASTER_PASSWORD", "********")
REDSHIFT_CLUSTER_DB_NAME = os.environ.get("REDSHIFT_CLUSTER_DB_NAME", "astro_dev")
AWS_CONN_ID = os.environ.get("ASTRO_AWS_CONN_ID", "aws_default")

default_args = {
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="example_async_redshift_cluster_management",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "redshift"],
) as dag:
    start = DummyOperator(task_id="start")

    # Execute AWS command then sleep for 5 min so that cluster would be available
    create_redshift_cluster = BashOperator(
        task_id="create_redshift_cluster",
        bash_command=f"aws redshift create-cluster "
        f"--db-name {REDSHIFT_CLUSTER_DB_NAME} "
        f"--cluster-identifier {REDSHIFT_CLUSTER_IDENTIFIER} "
        f"--cluster-type single-node "
        f"--node-type dc2.large  "
        f"--master-username {REDSHIFT_CLUSTER_MASTER_USER} "
        f"--master-user-password {REDSHIFT_CLUSTER_MASTER_PASSWORD} && sleep 4m && "
        f"aws redshift create-cluster-snapshot "
        f"--snapshot-identifier {REDSHIFT_CLUSTER_IDENTIFIER}-snapshot "
        f"--cluster-identifier {REDSHIFT_CLUSTER_IDENTIFIER} && sleep 5m",
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
    # [START howto_operator_redshift_cluster_sensor_async]

    delete_redshift_cluster = BashOperator(
        task_id="delete_redshift_cluster",
        bash_command=f"aws redshift delete-cluster "
        f"--cluster-identifier {REDSHIFT_CLUSTER_IDENTIFIER} --skip-final-cluster-snapshot && sleep 2m && "
        f"aws redshift delete-cluster-snapshot "
        f"--snapshot-identifier {REDSHIFT_CLUSTER_IDENTIFIER}-snapshot",
        trigger_rule="all_done",
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> create_redshift_cluster
        >> pause_cluster_task
        >> [resume_cluster_task, async_redshift_sensor_task]
        >> delete_redshift_cluster
        >> end
    )

    [resume_cluster_task, async_redshift_sensor_task] >> end
