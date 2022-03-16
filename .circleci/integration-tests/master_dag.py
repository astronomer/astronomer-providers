import time
from datetime import datetime
from typing import Dict, List

from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session


def get_report(dag_run_ids: List[str]) -> None:
    """Fetch dags run details and generate report"""
    with create_session() as session:
        last_dags_runs: List[DagRun] = session.query(DagRun).filter(DagRun.run_id.in_(dag_run_ids)).all()
        print(last_dags_runs)
        for dr in last_dags_runs:
            tis: Dict[str, TaskInstance] = {
                ti.task_id: ti
                for ti in dr.get_task_instances()
                if not ((ti.task_id == "end") or (ti.task_id == "get_report"))
            }
            print("task instance: ", tis)


with DAG(
    dag_id="example_master_dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["master_dag"],
) as dag:
    # Need to sleep for 30 sec so that all example dag will be available before master dag trigger it
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: time.sleep(30),
    )

    trigger_task_info = [
        {"redshift_cluster_mgmt_dag": "example_async_redshift_cluster_management"},
        {"redshift_sql_dag": "example_async_redshift_sql"},
        {"s3_sensor_dag": "example_s3_sensor"},
        {"kubernetes_pod_dag": "example_kubernetes_operator"},
        {"external_task_dag": "test_external_task_async"},
        {"external_task_wait_dag": "test_external_task_async_waits_for_me"},
        {"file_sensor_dag": "example_async_file_sensor"},
        {"databricks_dag": "example_async_databricks"},
        {"bigquery_dag": "example_async_bigquery_queries"},
        {"gcs_sensor_dag": "example_async_gcs_sensors"},
        {"http_dag": "example_snowflake"},
        {"snowflake_dag": "example_snowflake"},
    ]

    dag_run_ids = []
    trigger_tasks = []
    for example_dag in trigger_task_info:
        task_id = list(example_dag.keys())[0]

        run_id = f"{task_id}_{example_dag.get(task_id)}_" + "{{ds}}"
        dag_run_ids.append(run_id)
        trigger_tasks.append(
            TriggerDagRunOperator(
                task_id=task_id,
                trigger_dag_id=example_dag.get(task_id),
                trigger_run_id=run_id,
                wait_for_completion=True,
                reset_dag_run=True,
                execution_date="{{ ds }}",
                allowed_states=["success", "failed", "skipped"],
            )
        )

    report = PythonOperator(
        task_id="get_report",
        python_callable=get_report,
        op_kwargs={"dag_run_ids": dag_run_ids},
        trigger_rule="all_done",
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule="all_success",
    )

    start >> trigger_tasks >> report
    start >> trigger_tasks >> end
