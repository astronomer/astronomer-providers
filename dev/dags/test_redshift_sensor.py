import airflow
from airflow.utils.dates import days_ago

from astronomer_operators.amazon.aws.sensors.redshift_cluster import (
    RedshiftClusterSensorAsync,
)

with airflow.DAG(
    "example_async_redshift_sensor_task", start_date=days_ago(1), tags=["example", "async"]
) as dag:
    async_redshift_sensor_task = RedshiftClusterSensorAsync(
        task_id="redshift_sensor",
        cluster_identifier="astro-redshift-cluster-1",
        target_status="available",
    )
