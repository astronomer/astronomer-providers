import json
import os
from datetime import timedelta
from typing import Dict, Optional

from airflow.models.dag import DAG
from airflow.utils.timezone import datetime

from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)

DATABRICKS_CONN_ID = os.getenv("ASTRO_DATABRICKS_CONN_ID", "databricks_default")
# Notebook path as a Json object
notebook_task = '{"notebook_path": "/Users/phani.kumar@astronomer.io/quick_start"}'
NOTEBOOK_TASK = json.loads(os.getenv("DATABRICKS_NOTEBOOK_TASK", notebook_task))
notebook_params: Optional[Dict[str, str]] = {"Variable": "5"}
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

new_cluster = {
    "num_workers": 1,
    "spark_version": "10.4.x-scala2.12",  # Update to the desired Databricks runtime version
    "spark_conf": {},  # Additional Spark configuration options if needed
    "azure_attributes": {  # Azure-specific attributes
        "availability": "ON_DEMAND_AZURE",  # Optionally use SPOT_AZURE for spot instances
        "spot_bid_max_price": -1,  # Use the default spot pricing; set a max price if needed
    },
    "node_type_id": "Standard_D3_v2",  # Replace with an appropriate Azure VM type
    "ssh_public_keys": [],  # Add SSH public keys if you need SSH access to the nodes
    "custom_tags": {},  # Add custom tags if needed
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"  # Ensure the correct Python path
    },
    "cluster_source": "JOB",
    "init_scripts": [],  # Specify any initialization scripts if needed
}



with DAG(
    dag_id="example_async_databricks",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "databricks"],
) as dag:
    # [START howto_operator_databricks_submit_run_async]
    opr_submit_run = DatabricksSubmitRunOperatorAsync(
        task_id="submit_run",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster=new_cluster,
        notebook_task=NOTEBOOK_TASK,
        do_xcom_push=True,
    )
    # [END howto_operator_databricks_submit_run_async]

    # [START howto_operator_databricks_run_now_async]
    opr_run_now = DatabricksRunNowOperatorAsync(
        task_id="run_now",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id="{{ task_instance.xcom_pull(task_ids='submit_run', dag_id='example_async_databricks', key='job_id') }}",
        notebook_params=notebook_params,
    )
    # [END howto_operator_databricks_run_now_async]

opr_submit_run >> opr_run_now
