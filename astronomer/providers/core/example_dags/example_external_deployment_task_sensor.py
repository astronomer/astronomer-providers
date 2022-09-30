import os
from datetime import timedelta
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.external_task import (
    ExternalDeploymentTaskSensorAsync,
)

DEPLOYMENT_CONN_ID = os.getenv("ASTRO_DEPLOYMENT_CONN_ID", "deployment_conn_id")
ASTRONOMER_KEY_ID = os.getenv("ASTRONOMER_KEY_ID", "")
ASTRONOMER_KEY_SECRET = os.getenv("ASTRONOMER_KEY_SECRET", "")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
DAG_ID = os.getenv("DAG_ID", "")
RUN_ID = os.getenv("RUN_ID", "")
TASK_ID = os.getenv("TASK_ID", "")

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def astro_access_token() -> Dict[str, Any]:
    """Get the Headers with access token by making post request with client_id and client_secret"""
    conn = BaseHook.get_connection(DEPLOYMENT_CONN_ID)
    _json = {
        "audience": "astronomer-ee",
        "grant_type": "client_credentials",
        "client_id": ASTRONOMER_KEY_ID,
        "client_secret": ASTRONOMER_KEY_SECRET,
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
        "Authorization": "Bearer " + masked_access_token,
    }


with DAG(
    dag_id="example_async_deployment_task_sensor",
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

    # [START howto_sensor_external_deployment_task_async]
    external_deployment_task = ExternalDeploymentTaskSensorAsync(
        task_id="external_deployment_task",
        http_conn_id=DEPLOYMENT_CONN_ID,
        endpoint=f"/api/v1/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}",
        headers=generate_header_access_token.output,
        poke_interval=5,
    )
    # [END howto_sensor_external_deployment_task_async]

    generate_header_access_token >> external_deployment_task
