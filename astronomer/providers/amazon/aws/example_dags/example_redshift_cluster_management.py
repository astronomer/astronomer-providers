import os
from datetime import datetime, timedelta

from airflow import DAG
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

with DAG(
    dag_id="example_async_redshift_cluster_management",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "async", "redshift"],
) as dag:
    start = DummyOperator(task_id="start")

    # [START howto_operator_redshift_pause_cluster_async]
    pause_cluster_task = RedshiftPauseClusterOperatorAsync(
        task_id="pause_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
        execution_timeout=timedelta(seconds=60),
    )
    # [END howto_operator_redshift_pause_cluster_async]

    # [START howto_operator_redshift_resume_cluster_async]
    resume_cluster_task = RedshiftResumeClusterOperatorAsync(
        task_id="resume_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id=AWS_CONN_ID,
        execution_timeout=timedelta(seconds=60),
    )
    # [END howto_operator_redshift_resume_cluster_async]

    # [START howto_operator_redshift_cluster_sensor_async]
    async_redshift_sensor_task = RedshiftClusterSensorAsync(
        task_id="redshift_sensor",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        target_status="available",
        aws_conn_id=AWS_CONN_ID,
        execution_timeout=timedelta(seconds=60),
    )
    # [START howto_operator_redshift_cluster_sensor_async]

    end = DummyOperator(task_id="end")

    start >> pause_cluster_task >> [resume_cluster_task, async_redshift_sensor_task] >> end
