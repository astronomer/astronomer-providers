import logging
import os
import time
from datetime import datetime
from typing import List

from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import DagRun
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session

SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#provider-alert")
SLACK_WEBHOOK_CONN = os.environ.get("SLACK_WEBHOOK_CONN", "http_slack")
SLACK_USERNAME = os.environ.get("SLACK_USERNAME", "airflow_app")


def get_report(dag_run_ids: List[str]) -> None:
    """Fetch dags run details and generate report"""
    with create_session() as session:
        last_dags_runs: List[DagRun] = session.query(DagRun).filter(DagRun.run_id.in_(dag_run_ids)).all()
        message_list: List[str] = []
        for dr in last_dags_runs:
            dr_status = f" *{dr.dag_id} : {dr.get_state()}* \n"
            message_list.append(dr_status)
            for ti in dr.get_task_instances():
                task_code = ":black_circle: "
                if not ((ti.task_id == "end") or (ti.task_id == "get_report")):
                    if ti.state == "success":
                        task_code = ":large_green_circle: "
                    elif ti.state == "failed":
                        task_code = ":red_circle: "
                    elif ti.state == "upstream_failed":
                        task_code = ":large_orange_circle: "
                    task_message_str = f"{task_code} {ti.task_id} : {ti.state} \n"
                    message_list.append(task_message_str)

        logging.info("%s", "".join(message_list))
        # Send dag run report on Slack
        try:
            SlackWebhookOperator(
                task_id="slack_alert",
                http_conn_id=SLACK_WEBHOOK_CONN,
                message="".join(message_list),
                channel=SLACK_CHANNEL,
                username=SLACK_USERNAME,
            ).execute(context=None)
        except Exception:
            logging.exception("Error occur while sending slack alert.")


def prepare_dag_dependency(task_info, execution_time):
    """Prepare list of TriggerDagRunOperator task and dags run ids for dags of same providers"""
    _dag_run_ids = []
    _task_list = []
    for _example_dag in task_info:
        _task_id = list(_example_dag.keys())[0]

        _run_id = f"{_task_id}_{_example_dag.get(_task_id)}_" + execution_time
        _dag_run_ids.append(_run_id)
        _task_list.append(
            TriggerDagRunOperator(
                task_id=_task_id,
                trigger_dag_id=_example_dag.get(_task_id),
                trigger_run_id=_run_id,
                wait_for_completion=True,
                reset_dag_run=True,
                execution_date=execution_time,
                allowed_states=["success", "failed", "skipped"],
            )
        )
    return _task_list, _dag_run_ids


with DAG(
    dag_id="example_master_dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["master_dag"],
) as dag:
    # Sleep for 30 seconds so that all the example dag will be available before master dag trigger them
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: time.sleep(30),
    )

    dag_run_ids = []
    # AWS S3 and Redshift DAG
    amazon_task_info = [
        {"s3_sensor_dag": "example_s3_sensor"},
        {"redshift_sql_dag": "example_async_redshift_sql"},
        {"redshift_cluster_mgmt_dag": "example_async_redshift_cluster_management"},
    ]
    amazon_trigger_tasks, ids = prepare_dag_dependency(amazon_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*amazon_trigger_tasks)

    # AWS EMR DAG
    emr_task_info = [
        {"emr_sensor_dag": "example_emr_sensor"},
        {"emr_eks_pi_job_dag": "example_emr_eks_pi_job"},
    ]
    emr_trigger_tasks, ids = prepare_dag_dependency(emr_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*emr_trigger_tasks)

    # Google DAG
    google_task_info = [
        {"bigquery_dag": "example_async_bigquery_queries"},
        {"gcs_sensor_dag": "example_async_gcs_sensors"},
        {"big_query_sensor_dag": "example_bigquery_sensors"},
        {"dataproc_dag": "example_gcp_dataproc"},
    ]
    google_trigger_tasks, ids = prepare_dag_dependency(google_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*google_trigger_tasks)

    # Core DAG
    core_task_info = [
        {"external_task_wait_dag": "test_external_task_async_waits_for_me"},
        {"external_task_dag": "test_external_task_async"},
        {"file_sensor_dag": "example_async_file_sensor"},
    ]
    core_trigger_tasks, ids = prepare_dag_dependency(core_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*core_trigger_tasks)

    # CNCF Kubernetes DAG
    kubernetes_task_info = [{"kubernetes_pod_dag": "example_kubernetes_operator"}]
    kubernetes_trigger_tasks, ids = prepare_dag_dependency(kubernetes_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*kubernetes_trigger_tasks)

    # Databricks DAG
    databricks_task_info = [{"databricks_dag": "example_async_databricks"}]
    databricks_trigger_tasks, ids = prepare_dag_dependency(databricks_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*databricks_trigger_tasks)

    # HTTP DAG
    http_task_info = [{"http_dag": "example_async_http_sensor"}]
    http_trigger_tasks, ids = prepare_dag_dependency(http_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*http_trigger_tasks)

    # Snowflake DAG
    snowflake_task_info = [{"snowflake_dag": "example_snowflake"}]
    snowflake_trigger_tasks, ids = prepare_dag_dependency(snowflake_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*snowflake_trigger_tasks)

    # Apache livy DAG
    livy_task_info = [{"livy_dag": "example_livy_operator"}]
    livy_trigger_tasks, ids = prepare_dag_dependency(livy_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*livy_trigger_tasks)

    # Apache Hive Dag
    hive_task_info = [{"hive_dag": "example_hive"}]
    hive_trigger_tasks, ids = prepare_dag_dependency(hive_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*hive_trigger_tasks)

    # microsoft Azure Data factory pipeline DAG
    adf_pipeline_task_info = [{"adf_pipeline_dag": "example_async_adf_run_pipeline"}]
    adf_pipeline_trigger_tasks, ids = prepare_dag_dependency(adf_pipeline_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*adf_pipeline_trigger_tasks)

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

    start >> [
        amazon_trigger_tasks[0],
        emr_trigger_tasks[0],
        google_trigger_tasks[0],
        core_trigger_tasks[0],
        kubernetes_trigger_tasks[0],
        databricks_trigger_tasks[0],
        http_trigger_tasks[0],
        snowflake_trigger_tasks[0],
        livy_trigger_tasks[0],
        hive_trigger_tasks[0],
        adf_pipeline_trigger_tasks[0],
    ]

    last_task = [
        amazon_trigger_tasks[-1],
        emr_trigger_tasks[-1],
        google_trigger_tasks[-1],
        core_trigger_tasks[-1],
        kubernetes_trigger_tasks[-1],
        databricks_trigger_tasks[-1],
        http_trigger_tasks[-1],
        snowflake_trigger_tasks[-1],
        livy_trigger_tasks[-1],
        hive_trigger_tasks[-1],
        adf_pipeline_trigger_tasks[-1],
    ]

    last_task >> end
    last_task >> report
