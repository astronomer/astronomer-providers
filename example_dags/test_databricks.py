# Copyright 2022 Astronomer Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

from astronomer_operators.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)

notebook_task = {"notebook_path": "/Users/andrew.godwin@astronomer.io/Quickstart Notebook"}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    "databricks_dag",
    start_date=days_ago(0),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    opr_submit_run = DatabricksSubmitRunOperatorAsync(
        task_id="submit_run",
        databricks_conn_id="databricks_default",
        existing_cluster_id="0806-193014-swab896",
        notebook_task=notebook_task,
        polling_period_seconds=30,
    )

    opr_run_now = DatabricksRunNowOperatorAsync(
        task_id="run_now",
        databricks_conn_id="databricks_default",
        job_id=1003,
        polling_period_seconds=30,
    )

    opr_submit_run >> opr_run_now
