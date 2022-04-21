"""This is an example dag for hive partition sensors."""
import os
from datetime import datetime

from airflow import DAG

from astronomer.providers.apache.hive.sensors.hive_partition import (
    HivePartitionSensorAsync,
)

HIVE_TABLE = os.environ.get("HIVE_TABLE", "zipcode")
HIVE_PARTITION = os.environ.get("HIVE_PARTITION", "state='FL'")

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
        table=HIVE_TABLE,
        partition=HIVE_PARTITION,
        poke_interval=5,
    )
    # [END howto_hive_partition_check]
