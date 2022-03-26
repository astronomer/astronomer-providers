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

REDSHIFT_CLUSTER_IDENTIFIER = os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER", "astro-redshift-cluster-1")
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
        bash_command="aws redshift create-cluster "
        "--db-name dev "
        "--cluster-identifier redshift-cluster-1 "
        "--cluster-type single-node "
        "--node-type dc2.large  "
        "--master-username adminuser "
        "--master-user-password TopSecret1 && sleep 5m",
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
        bash_command="aws redshift delete-cluster "
        "--cluster-identifier redshift-cluster-1 --skip-final-cluster-snapshot && sleep 2m",
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
