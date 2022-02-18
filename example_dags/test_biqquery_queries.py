#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG for Google BigQuery service.
Uses Async version of BigQueryInsertJobOperator.
Uses Async version of BigQueryGetDataOperatorAsync
"""
from datetime import datetime

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)

from astronomer_operators.google.cloud.operators.bigquery import (
    BigQueryGetDataOperatorAsync,
    BigQueryInsertJobOperatorAsync,
)

PROJECT_ID = "astronomer-airflow-providers"
DATASET = "astro_dataset_test"
LOCATION = "us"

TABLE_1 = "table1"

SCHEMA = [
    {"name": "value", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "DATE", "mode": "NULLABLE"},
]

location = LOCATION
dag_id = "example_async_bigquery_getdata_job"
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {DATASET}.{TABLE_1} VALUES "
    f"(42, 'monthy python', '{INSERT_DATE}'), "
    f"(42, 'fishy fish', '{INSERT_DATE}');"
)

with models.DAG(
    dag_id,
    schedule_interval="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
    user_defined_macros={"DATASET": DATASET, "TABLE": TABLE_1},
) as dag_with_locations:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create-dataset",
        dataset_id=DATASET,
        location=location,
    )

    create_table_1 = BigQueryCreateEmptyTableOperator(
        task_id="create_table_1",
        dataset_id=DATASET,
        table_id=TABLE_1,
        schema_fields=SCHEMA,
        location=location,
    )

    insert_query_job = BigQueryInsertJobOperatorAsync(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
        location=location,
    )

    get_data = BigQueryGetDataOperatorAsync(
        task_id="get_data",
        dataset_id=DATASET,
        table_id=TABLE_1,
        location=location,
    )

    get_data_result = BashOperator(
        task_id="get_data_result",
        bash_command=f"echo {get_data.output}",
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET, delete_contents=True
    )

    create_dataset >> create_table_1 >> insert_query_job >> get_data >> get_data_result >> delete_dataset
