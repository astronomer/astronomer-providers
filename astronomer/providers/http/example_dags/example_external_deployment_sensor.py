import os
from datetime import timedelta

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.log.secrets_masker import mask_secret
from airflow.utils.timezone import datetime

from astronomer.providers.http.sensors.external_deployment_task import (
    ExternalDeploymentTaskSensorAsync,
)

DEPLOYMENT_CONN_ID = os.getenv("ASTRO_DEPLOYMENT_CONN_ID", "http_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def astro_access_token():
    """Get the access_token by making post request with client_id"""
    conn = BaseHook.get_connection(DEPLOYMENT_CONN_ID)
    _json = {
        "audience": "astronomer-ee",
        "grant_type": "client_credentials",
    }
    _json.update(conn.extra_dejson)
    token_resp = requests.post(
        url=conn.host,
        headers={"Content-type": "application/json"},
        json=_json,
    )
    masked_access_token = token_resp.json()["access_token"]
    mask_secret(masked_access_token)
    return {"Authorization": "Bearer " + masked_access_token}


with DAG(
    dag_id="example_async_http_sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "http"],
) as dag:

    # [START howto_sensor_external_deployment_task_async]
    external_deployment_task = ExternalDeploymentTaskSensorAsync(
        task_id="external_deployment_task",
        http_conn_id=DEPLOYMENT_CONN_ID,
        endpoint="/api/v1/dags/1234/dagRuns/123/taskInstances/finish",
        request_params={},
        headers={
            "cache-control": "no-cache",
            "content-type": "application/json",
            "accept": "application/json",
            "Authorization": "Bearer ",
        },
        poke_interval=5,
    )
    # [END howto_sensor_external_deployment_task_async]

    external_deployment_task
