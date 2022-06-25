import os
import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.timezone import datetime

from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_external_task",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "core"],
) as dag:
    start = DummyOperator(task_id="start")

    # [START howto_sensor_external_task_async]
    waiting_for_task = ExternalTaskSensorAsync(
        task_id="waiting_for_task",
        external_task_id="wait_for_me",
        external_dag_id="example_external_task",
    )
    # [END howto_sensor_external_task_async]

    wait_for_me = PythonOperator(
        task_id="wait_for_me",
        python_callable=lambda: time.sleep(5),
    )

    # When ``external_task_id`` not provided
    wait_for_dag = ExternalTaskSensorAsync(
        task_id="wait_for_dag",
        external_dag_id="example_external_task_async_waits_for_me",
    )

    wait_to_defer = PythonOperator(
        task_id="wait_to_defer",
        python_callable=lambda: time.sleep(5),
    )

    external = TriggerDagRunOperator(
        task_id="external_dag",
        trigger_dag_id="example_external_task_async_waits_for_me",
        wait_for_completion=True,
        reset_dag_run=True,
        allowed_states=["success", "failed", "skipped"],
        execution_date="{{execution_date}}",
        poke_interval=1,
    )

    end = DummyOperator(task_id="end")

    start >> [waiting_for_task, wait_for_me] >> end

    start >> wait_for_dag >> end
    start >> wait_to_defer >> external >> end
