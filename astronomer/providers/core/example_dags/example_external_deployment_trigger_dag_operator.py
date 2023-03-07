import os
from datetime import timedelta
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.timezone import datetime

from astronomer.providers.core.operators.external_trigger_dagrun import (
    ExternalDeploymentTriggerDagRunOperator,
)

DEPLOYMENT_CONN_ID = os.getenv("ASTRO_DEPLOYMENT_CONN_ID", "")
ASTRO_AUTH_CONN_ID = os.getenv("ASTRO_AUTH_CONN_ID", "")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
TRIGGER_DAG_ID = os.getenv("TRIGGER_DAG_ID", "")
TRIGGER_RUN_ID = os.getenv("TRIGGER_RUN_ID", "")

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def astro_access_token() -> Dict[str, Any]:
    """Get the Headers with access token by making post request with client_id and client_secret"""
    conn = BaseHook.get_connection(ASTRO_AUTH_CONN_ID)
    _json = {
        "audience": "astronomer-ee",
        "grant_type": "client_credentials",
        "client_id": conn.login,
        "client_secret": conn.get_password(),
    }
    token_resp = requests.post(
        url=conn.host,
        headers={"Content-type": "application/json"},
        json=_json,
    )
    masked_access_token = token_resp.json()["access_token"]
    mask_secret(masked_access_token)
    return {
        "cache-control": "no-cache",
        "content-type": "application/json",
        "accept": "application/json",
        "Authorization": f"Bearer {masked_access_token}",
    }


with DAG(
    dag_id="example_external_deployment_trigger_dag_operator",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "http"],
) as dag:
    # Task to Generate headers access token
    generate_header_access_token = PythonOperator(
        task_id="generate_header_access_token",
        python_callable=astro_access_token,
    )

    # [START howto_external_deployment_trigger_dag_run]
    external_deployment_trigger_dag_run = ExternalDeploymentTriggerDagRunOperator(
        task_id="external_deployment_trigger_dag_run",
        http_conn_id=DEPLOYMENT_CONN_ID,
        trigger_dag_id=TRIGGER_DAG_ID,
        trigger_run_id=TRIGGER_RUN_ID,
        conf={},
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=True,
        headers=generate_header_access_token.output,
        poke_interval=5,
    )
    # [END howto_external_deployment_trigger_dag_run]

    generate_header_access_token >> external_deployment_trigger_dag_run
