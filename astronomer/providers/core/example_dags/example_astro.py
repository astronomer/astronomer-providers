import time
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from astronomer.providers.core.sensors.astro import ExternalDeploymentSensor

with DAG(
    dag_id="example_astro_task",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "async", "core"],
):
    ExternalDeploymentSensor(
        task_id="test1",
        external_dag_id="example_wait_to_test_example_astro_task",
    )

    ExternalDeploymentSensor(
        task_id="test2",
        external_dag_id="example_wait_to_test_example_astro_task",
        external_task_id="wait_for_2_min",
    )

with DAG(
    dag_id="example_wait_to_test_example_astro_task",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "async", "core"],
):

    @task
    def wait_for_2_min() -> None:
        """Wait for 2 min."""
        time.sleep(120)

    wait_for_2_min()


with DAG(
    dag_id="trigger_astro_test_and_example",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "async", "core"],
):
    run_wait_dag = TriggerDagRunOperator(
        task_id="run_wait_dag",
        trigger_dag_id="example_wait_to_test_example_astro_task",
        wait_for_completion=False,
    )

    run_astro_dag = TriggerDagRunOperator(
        task_id="run_astro_dag",
        trigger_dag_id="example_astro_task",
        wait_for_completion=False,
    )

    run_wait_dag >> run_astro_dag
