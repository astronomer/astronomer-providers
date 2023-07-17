"""Master Dag to run all the example dags."""
import logging
import os
import time
from datetime import datetime
from typing import Any, List

from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import DagRun
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import create_session
from airflow_dag_introspection import log_checker

SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#provider-alert")
SLACK_WEBHOOK_CONN = os.getenv("SLACK_WEBHOOK_CONN", "http_slack")
SLACK_USERNAME = os.getenv("SLACK_USERNAME", "airflow_app")
MASTER_DAG_SCHEDULE = os.getenv("MASTER_DAG_SCHEDULE", None)
IS_RUNTIME_RELEASE = bool(os.getenv("IS_RUNTIME_RELEASE", False))

"""
To run this master dag across multiple deployments simultaneously, set unique values for the below environment
variables; otherwise same resources will be used by all the deployments and will cause conflicts at runtime.

ADF_FACTORY_NAME
AZURE_DATA_STORAGE_BLOB_NAME
AZURE_DATA_STORAGE_CONTAINER_NAME
BATCH_JOB_COMPUTE_ENV
BATCH_JOB_NAME
BATCH_JOB_QUEUE
EKS_CLUSTER_NAME
EKS_NAMESPACE
EMR_VIRTUAL_CLUSTER_NAME
GCP_BIGQUERY_DATASET_NAME
GCP_DATAPROC_CLUSTER_NAME
GCP_TEST_BUCKET
GKE_CLUSTER_NAME
GKE_POD_NAME
HIVE_CLUSTER
JOB_EXECUTION_ROLE
LIVY_CLUSTER
MODEL_NAME
REDSHIFT_CLUSTER_IDENTIFIER
REDSHIFT_TABLE_NAME
RESOURCE_GROUP_NAME


Additionally, ensure to have unique "cluster_identifier" values for each deployment in the below Airflow connections:
aws_default
redshift_default
"""


def get_report(dag_run_ids: List[str], **context: Any) -> None:
    """Fetch dags run details and generate report."""
    with create_session() as session:
        last_dags_runs: List[DagRun] = session.query(DagRun).filter(DagRun.run_id.in_(dag_run_ids)).all()
        message_list: List[str] = []

        airflow_version = context["ti"].xcom_pull(task_ids="get_airflow_version")
        airflow_executor = context["ti"].xcom_pull(task_ids="get_airflow_executor")
        astronomer_providers_version = context["ti"].xcom_pull(task_ids="get_astronomer_providers_version")
        astro_cloud_provider = context["ti"].xcom_pull(task_ids="get_astro_cloud_provider")

        if IS_RUNTIME_RELEASE:
            airflow_version_message = f"Results generated for Runtime version `{os.environ['ASTRONOMER_RUNTIME_VERSION']}` with `{airflow_executor}` and astronomer-providers version `{astronomer_providers_version}` and cloud provider `{astro_cloud_provider}`\n\n"
        else:
            airflow_version_message = f"The below run is on Airflow version `{airflow_version}` with `{airflow_executor}` and astronomer-providers version `{astronomer_providers_version}` and cloud provider `{astro_cloud_provider}`\n\n"

        master_dag_deployment_link = f"{os.environ['AIRFLOW__WEBSERVER__BASE_URL']}/dags/example_master_dag/grid?search=example_master_dag"
        deployment_message = f"\n <{master_dag_deployment_link}|Link> to the master DAG for the above run on Astro Cloud deployment \n"

        dag_count, failed_dag_count = 0, 0
        for dr in last_dags_runs:
            dr_status = f" *{dr.dag_id} : {dr.get_state()}* \n"
            dag_count += 1
            failed_tasks = []
            for ti in dr.get_task_instances():
                task_code = ":black_circle: "
                if not ((ti.task_id == "end") or (ti.task_id == "get_report")):
                    if ti.state == "success":
                        continue
                    elif ti.state == "failed":
                        task_code = ":red_circle: "
                        failed_tasks.append(f"{task_code} {ti.task_id} : {ti.state} \n")
                    elif ti.state == "upstream_failed":
                        task_code = ":large_orange_circle: "
                        failed_tasks.append(f"{task_code} {ti.task_id} : {ti.state} \n")
                    else:
                        failed_tasks.append(f"{task_code} {ti.task_id} : {ti.state} \n")
            if failed_tasks:
                message_list.append(dr_status)
                message_list.extend(failed_tasks)
                failed_dag_count += 1
        output_list = [
            airflow_version_message,
            f"*Total DAGS*: {dag_count} \n",
            f"*Success DAGS*: {dag_count-failed_dag_count} :green_apple: \n",
            f"*Failed DAGS*: {failed_dag_count} :apple: \n \n",
        ]
        if failed_dag_count > 0:
            output_list.append("*Failure Details:* \n")
            output_list.extend(message_list)
        dag_run = context["dag_run"]
        task_instances = dag_run.get_task_instances()

        task_failure_message_list: List[str] = [
            f":red_circle: {ti.task_id} \n" for ti in task_instances if ti.state == "failed"
        ]

        if task_failure_message_list:
            output_list.append(
                "\nSome of Master DAG tasks failed, please check with deployment link below \n"
            )
            output_list.extend(task_failure_message_list)
        output_list.append(deployment_message)
        logging.info("%s", "".join(output_list))
        # Send dag run report on Slack
        try:
            SlackWebhookOperator(
                task_id="slack_alert",
                http_conn_id=SLACK_WEBHOOK_CONN,
                message="".join(output_list),
                channel=SLACK_CHANNEL,
                username=SLACK_USERNAME,
            ).execute(context=None)
        except Exception as exception:
            logging.exception("Error occur while sending slack alert.")
            raise exception


def prepare_dag_dependency(task_info, execution_time):
    """Prepare list of TriggerDagRunOperator task and dags run ids for dags of same providers."""
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
                allowed_states=["success", "failed"],
                trigger_rule="all_done",
            )
        )
    return _task_list, _dag_run_ids


with DAG(
    dag_id="example_master_dag",
    schedule=MASTER_DAG_SCHEDULE,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["master_dag"],
) as dag:
    # Sleep for 30 seconds so that all the example dag will be available before master dag trigger them
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: time.sleep(30),
    )

    list_installed_pip_packages = BashOperator(
        task_id="list_installed_pip_packages", bash_command="pip freeze"
    )

    get_airflow_version = BashOperator(
        task_id="get_airflow_version", bash_command="airflow version", do_xcom_push=True
    )
    check_logs_data = PythonOperator(
        task_id="check_logs",
        python_callable=log_checker,
        op_args=[
            "get_airflow_version",
            "{{ ti.xcom_pull(task_ids='get_airflow_version') }}",
            "this_string_should_not_be_present_in_logs",
        ],
    )

    airflow_version_check = (get_airflow_version, check_logs_data)
    chain(*airflow_version_check)

    get_airflow_executor = BashOperator(
        task_id="get_airflow_executor",
        bash_command="airflow config get-value core executor",
        do_xcom_push=True,
    )

    get_astronomer_providers_version = BashOperator(
        task_id="get_astronomer_providers_version",
        bash_command="airflow providers get astronomer-providers -o json | jq '.[0].Version'",
        do_xcom_push=True,
    )

    get_astro_cloud_provider = BashOperator(
        task_id="get_astro_cloud_provider",
        bash_command="[[ $AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID == *azure* ]] && echo 'azure' || ([[ $AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID == *s3* ]] && echo 'aws' || ([[ $AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID == *gcs* ]] && echo 'gcs' || echo 'unknown'))",
        do_xcom_push=True,
    )

    dag_run_ids = []

    # AWS sagemaker and batch
    aws_misc_dags_info = [
        {"sagemaker_dag": "example_async_sagemaker"},
        {"batch_dag": "example_async_batch"},
    ]
    aws_misc_dags_tasks, ids = prepare_dag_dependency(aws_misc_dags_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*aws_misc_dags_tasks)

    # AWS S3 and Redshift DAG
    amazon_task_info = [
        {"redshift_cluster_mgmt_dag": "example_async_redshift_cluster_management"},
        {"redshift_sql_dag": "example_async_redshift_sql"},
        {"redshift_data_dag": "example_async_redshift_data"},
        {"s3_sensor_dag": "example_s3_sensor"},
    ]
    amazon_trigger_tasks, ids = prepare_dag_dependency(amazon_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*amazon_trigger_tasks)

    # AWS EMR EKS PI DAG
    emr_eks_task_info = [
        {"emr_eks_pi_job_dag": "example_emr_eks_pi_job"},
    ]
    emr_eks_trigger_tasks, ids = prepare_dag_dependency(emr_eks_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*emr_eks_trigger_tasks)

    # AWS EMR Sensor DAG
    emr_sensor_task_info = [
        {"emr_sensor_dag": "example_emr_sensor"},
    ]
    emr_sensor_trigger_tasks, ids = prepare_dag_dependency(emr_sensor_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*emr_sensor_trigger_tasks)

    # Google DAG
    google_task_info = [
        {"bigquery_dag": "example_async_bigquery_queries"},
        {"gcs_sensor_dag": "example_async_gcs_sensors"},
        {"big_query_sensor_dag": "example_bigquery_sensors"},
        {"dataproc_dag": "example_gcp_dataproc"},
        {"kubernetes_engine_dag": "example_google_kubernetes_engine"},
        {"bigquery_impersonation_dag": "example_bigquery_impersonation"},
        {"dataproc_impersonation_dag": "example_gcp_dataproc_impersonation"},
    ]
    google_trigger_tasks, ids = prepare_dag_dependency(google_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*google_trigger_tasks)

    # Core DAG
    core_task_info = [
        {"external_task_dag": "example_external_task"},
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
    snowflake_task_info = [
        {"snowflake_dag": "example_snowflake"},
        {"snowflake_sql_api_dag": "example_snowflake_sql_api"},
        {"example_snowflake_sensor": "example_snowflake_sensor"},
    ]
    snowflake_trigger_tasks, ids = prepare_dag_dependency(snowflake_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*snowflake_trigger_tasks)

    # Apache livy DAG
    livy_task_info = [{"livy_dag": "example_livy_operator"}]
    livy_trigger_tasks, ids = prepare_dag_dependency(livy_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*livy_trigger_tasks)

    # Apache Hive Dag
    hive_task_info = [{"hive_dag": "example_hive_dag"}]
    hive_trigger_tasks, ids = prepare_dag_dependency(hive_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*hive_trigger_tasks)

    # Microsoft Azure DAGs
    azure_task_info = [
        {"wasb_sensors_dag": "example_wasb_sensors"},
        {"adf_pipeline_dag": "example_async_adf_run_pipeline"},
    ]
    azure_trigger_tasks, ids = prepare_dag_dependency(azure_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*azure_trigger_tasks)

    # SFTP
    sftp_task_info = [
        {"sftp_dag": "example_async_sftp_sensor"},
    ]
    sftp_trigger_tasks, ids = prepare_dag_dependency(sftp_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*sftp_trigger_tasks)

    # DBT
    dbt_task_info = [
        {"dbt_dag": "example_dbt_cloud"},
    ]
    dbt_trigger_tasks, ids = prepare_dag_dependency(dbt_task_info, "{{ ds }}")
    dag_run_ids.extend(ids)
    chain(*dbt_trigger_tasks)

    report = PythonOperator(
        task_id="get_report",
        python_callable=get_report,
        op_kwargs={"dag_run_ids": dag_run_ids},
        trigger_rule="all_done",
        provide_context=True,
    )

    end = DummyOperator(
        task_id="end",
        trigger_rule="all_success",
    )

    start >> [
        list_installed_pip_packages,
        airflow_version_check[0],
        get_airflow_executor,
        get_astronomer_providers_version,
        get_astro_cloud_provider,
        emr_eks_trigger_tasks[0],
        emr_sensor_trigger_tasks[0],
        aws_misc_dags_tasks[0],
        amazon_trigger_tasks[0],
        google_trigger_tasks[0],
        core_trigger_tasks[0],
        kubernetes_trigger_tasks[0],
        databricks_trigger_tasks[0],
        http_trigger_tasks[0],
        snowflake_trigger_tasks[0],
        livy_trigger_tasks[0],
        hive_trigger_tasks[0],
        azure_trigger_tasks[0],
        sftp_trigger_tasks[0],
        dbt_trigger_tasks[0],
    ]

    last_task = [
        list_installed_pip_packages,
        airflow_version_check[-1],
        get_airflow_executor,
        get_astronomer_providers_version,
        get_astro_cloud_provider,
        amazon_trigger_tasks[-1],
        emr_eks_trigger_tasks[-1],
        emr_sensor_trigger_tasks[-1],
        aws_misc_dags_tasks[-1],
        google_trigger_tasks[-1],
        core_trigger_tasks[-1],
        kubernetes_trigger_tasks[-1],
        databricks_trigger_tasks[-1],
        http_trigger_tasks[-1],
        snowflake_trigger_tasks[-1],
        livy_trigger_tasks[-1],
        hive_trigger_tasks[-1],
        azure_trigger_tasks[-1],
        sftp_trigger_tasks[-1],
        dbt_trigger_tasks[-1],
    ]

    last_task >> report >> end
