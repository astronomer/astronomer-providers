import os

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.timezone import datetime

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)
from astronomer.providers.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)

REDSHIFT_CLUSTER_IDENTIFIER = os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER", "astro-redshift-cluster-1")

with DAG(
    dag_id="example_async_redshift_cluster_management",
    start_date=datetime(2021, 1, 1),
    tags=["example", "async"],
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")

    pause_cluster_task = RedshiftPauseClusterOperatorAsync(
        task_id="pause_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id="aws_default",
    )

    resume_cluster_task = RedshiftResumeClusterOperatorAsync(
        task_id="resume_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id="aws_default",
    )

    async_redshift_sensor_task = RedshiftClusterSensorAsync(
        task_id="redshift_sensor",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        target_status="available",
    )

    end = DummyOperator(task_id="end")


start >> pause_cluster_task >> [resume_cluster_task, async_redshift_sensor_task] >> end
