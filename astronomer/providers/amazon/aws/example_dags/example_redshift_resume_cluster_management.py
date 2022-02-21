import os

import airflow
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from astronomer.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftResumeClusterOperatorAsync,
)

REDSHIFT_CLUSTER_IDENTIFIER = os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER", "astro-redshift-cluster-1")

with airflow.DAG(
    dag_id="example_async_redshift_cluster_management",
    start_date=days_ago(1),
    tags=["example", "async"],
    schedule_interval="@once",
    catchup=False,
) as dag:

    start = DummyOperator(task_id="start")

    resume_cluster_task = RedshiftResumeClusterOperatorAsync(
        task_id="resume_redshift_cluster",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        aws_conn_id="aws_default",
    )

    end = DummyOperator(task_id="end")


start >> resume_cluster_task >> end
