"""Example Airflow DAG which uses impersonation parameters for authenticating with Google BigQuery service."""

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

from astronomer.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperatorAsync,
    BigQueryInsertJobOperatorAsync,
)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "astronomer-airflow-providers")
DATASET_NAME = os.getenv("GCP_BIGQUERY_DATASET_NAME", "astro_dataset")
GCP_IMPERSONATION_CONN_ID = os.getenv("GCP_IMPERSONATION_CONN_ID", "google_impersonation")
LOCATION = os.getenv("GCP_LOCATION", "us")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
IMPERSONATION_CHAIN = os.getenv("IMPERSONATION_CHAIN", "")

TABLE_1 = "table1"
TABLE_2 = "table2"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "STRING", "mode": "NULLABLE"},
]

DATASET = DATASET_NAME
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET}.{TABLE_1} VALUES "
    f"(42, 'monthy python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def check_dag_status(**kwargs: Any) -> None:
    """Raise an exception if any of the DAG's tasks failed and as a result marking the DAG failed."""
    for task_instance in kwargs["dag_run"].get_task_instances():
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")


with DAG(
    dag_id="example_bigquery_impersonation",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "bigquery"],
    user_defined_macros={"DATASET": DATASET, "TABLE": TABLE_1},
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET,
        location=LOCATION,
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )

    create_table_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_1",
        dataset_id=DATASET,
        table_id=TABLE_1,
        schema_fields=SCHEMA,
        location=LOCATION,
        bigquery_conn_id=GCP_IMPERSONATION_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )

    create_dataset >> create_table_1

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET,
        delete_contents=True,
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        trigger_rule="all_done",
        impersonation_chain=IMPERSONATION_CHAIN,
    )

    # [START howto_operator_bigquery_insert_job_async]
    insert_query_job = BigQueryInsertJobOperatorAsync(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_operator_bigquery_insert_job_async]

    # [START howto_operator_bigquery_select_job_async]
    select_query_job = BigQueryInsertJobOperatorAsync(
        task_id="select_query_job",
        configuration={
            "query": {
                "query": "{% include 'example_bigquery_query.sql' %}",
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
    )
    # [END howto_operator_bigquery_select_job_async]

    # [START howto_operator_bigquery_check_async]
    check_count = BigQueryCheckOperatorAsync(
        task_id="check_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE_1}",
        use_legacy_sql=False,
        location=LOCATION,
        gcp_conn_id=GCP_IMPERSONATION_CONN_ID,
    )
    # [END howto_operator_bigquery_check_async]

    dag_final_status = PythonOperator(
        task_id="dag_final_status",
        provide_context=True,
        python_callable=check_dag_status,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        dag=dag,
        retries=0,
    )

    (
        create_table_1
        >> insert_query_job
        >> select_query_job
        >> check_count
        >> delete_dataset
        >> dag_final_status
    )
