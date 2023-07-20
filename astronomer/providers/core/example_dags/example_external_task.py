import os
import time
from datetime import timedelta
from typing import Any

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.trigger_rule import TriggerRule

from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def check_dag_status(**kwargs: Any) -> None:
    """Raises an exception if any of the DAG's tasks failed and as a result marking the DAG failed."""
    for task_instance in kwargs["dag_run"].get_task_instances():
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")


with DAG(
    dag_id="example_external_task",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "core"],
) as dag:
    start = EmptyOperator(task_id="start")

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
        allowed_states=["success", "failed"],
        execution_date="{{execution_date}}",
        poke_interval=1,
    )

    dag_final_status = PythonOperator(
        task_id="dag_final_status",
        provide_context=True,
        python_callable=check_dag_status,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        retries=0,
    )

    start >> [waiting_for_task, wait_for_me] >> dag_final_status

    start >> wait_for_dag >> dag_final_status
    start >> wait_to_defer >> external >> dag_final_status
