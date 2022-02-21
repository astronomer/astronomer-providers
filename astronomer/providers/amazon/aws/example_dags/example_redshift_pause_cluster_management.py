import airflow
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
)

with airflow.DAG(
    dag_id="example_async_redshift_cluster_management",
    start_date=days_ago(1),
    tags=["example", "async"],
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = DummyOperator(task_id="start")

    pause_cluster_task = RedshiftPauseClusterOperatorAsync(
        task_id="pause_redshift_cluster",
        cluster_identifier="astro-redshift-cluster-1",
        aws_conn_id="aws_default",
    )

    end = DummyOperator(task_id="end")


start >> pause_cluster_task >> end
