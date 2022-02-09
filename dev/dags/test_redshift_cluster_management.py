import airflow
from airflow.utils.dates import days_ago

from astronomer_operators.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperatorAsync,
    RedshiftResumeClusterOperatorAsync,
)

with airflow.DAG(
    "example_async_redshift_cluster_management", start_date=days_ago(1), tags=["example", "async"]
) as dag:
    pause_cluster_task = RedshiftPauseClusterOperatorAsync(
        task_id="pause_redshift_cluster",
        cluster_identifier="astro-redshift-cluster-1",
    )

    resume_cluster_task = RedshiftResumeClusterOperatorAsync(
        task_id="resume_redshift_cluster",
        cluster_identifier="astro-redshift-cluster-1",
    )
