import os

import airflow
from airflow.utils.dates import days_ago

from astronomer.providers.amazon.aws.sensors.redshift_cluster_sensor import (
    RedshiftClusterSensorAsync,
)

REDSHIFT_CLUSTER_IDENTIFIER = os.environ.get("REDSHIFT_CLUSTER_IDENTIFIER", "astro-redshift-cluster-1")

with airflow.DAG(
    dag_id="example_async_redshift_sensor_task",
    start_date=days_ago(1),
    tags=["example", "async"],
    schedule_interval="@once",
    catchup=False,
) as dag:
    async_redshift_sensor_task = RedshiftClusterSensorAsync(
        task_id="redshift_sensor",
        cluster_identifier=REDSHIFT_CLUSTER_IDENTIFIER,
        target_status="available",
    )
