"""Example Airflow DAG for Google BigQuery Sensors."""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTablePartitionExistenceSensor,
)

from astronomer.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensorAsync,
)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "astronomer-airflow-providers")
DATASET_NAME = os.getenv("GCP_BIGQUERY_DATASET_NAME", "astro_dataset")
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
LOCATION = os.getenv("GCP_LOCATION", "us")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

TABLE_NAME = os.getenv("TABLE_NAME", "partitioned_table")
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")

PARTITION_NAME = "{{ ds_nodash }}"

INSERT_ROWS_QUERY = f"INSERT {DATASET_NAME}.{TABLE_NAME} VALUES " "(42, '{{ ds }}')"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "gcp_conn_id": GCP_CONN_ID,
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_bigquery_sensors",
    schedule_interval=None,  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "bigquery", "sensors"],
) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=SCHEMA,
        time_partitioning={
            "type": "DAY",
            "field": "ds",
        },
    )
    # [START howto_sensor_bigquery_table_existence_async]
    check_table_exists = BigQueryTableExistenceSensorAsync(
        task_id="check_table_exists", project_id=PROJECT_ID, dataset_id=DATASET_NAME, table_id=TABLE_NAME
    )
    # [END howto_sensor_bigquery_table_existence_async]

    execute_insert_query = BigQueryInsertJobOperator(
        task_id="execute_insert_query",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
    )

    # [START howto_sensor_bigquery_table_partition]
    check_table_partition_exists = BigQueryTablePartitionExistenceSensor(
        task_id="check_table_partition_exists",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        partition_id=PARTITION_NAME,
    )
    # [END howto_sensor_bigquery_table_partition]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )

    create_dataset >> create_table
    create_table >> check_table_exists
    create_table >> execute_insert_query
    execute_insert_query >> check_table_partition_exists
    check_table_exists >> delete_dataset
    check_table_partition_exists >> delete_dataset
