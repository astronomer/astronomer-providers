"""This is an example dag for hive partition sensors."""
from datetime import datetime

from airflow import DAG

from astronomer.providers.apache.hive.sensors.hive_partition import (
    HivePartitionSensorAsync,
)

with DAG(
    dag_id="example_hive_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["example", "async", "hive", "hive_partition"],
    catchup=False,
) as dag:
    # [START howto_hive_partition_check]
    op = HivePartitionSensorAsync(
        task_id="hive_partition_check",
        table="zipcode",
        partition="state='FL'",
    )
    # [END howto_hive_partition_check]
