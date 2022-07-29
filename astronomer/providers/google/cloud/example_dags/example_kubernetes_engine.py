import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
)

from astronomer.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperatorAsync,
)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "astronomer-airflow-providers")
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
LOCATION = os.getenv("GCP_GKE_LOCATION", "us-central1")
GKE_CLUSTER_NAME = os.getenv("GKE_CLUSTER_NAME", "provider-team-gke-cluster")
GKE_NAMESPACE = os.getenv("GKE_NAMESPACE", "default")

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}


with DAG(
    dag_id="example_google_kubernetes_engine",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "gke"],
) as dag:
    cluster_create = GKECreateClusterOperator(
        task_id="cluster_create",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        body={"name": GKE_CLUSTER_NAME, "initial_node_count": 1},
    )

    # [START howto_operator_gke_start_pod_async]
    start_pod = GKEStartPodOperatorAsync(
        task_id="start_pod",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_name=GKE_CLUSTER_NAME,
        name="astro_k8s_gke_test_pod",
        namespace=GKE_NAMESPACE,
        image="ubuntu",
        in_cluster=False,
        gcp_conn_id=GCP_CONN_ID,
    )
    # [END howto_operator_gke_start_pod_async]

    cluster_delete = GKEDeleteClusterOperator(
        task_id="cluster_delete",
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        name=GKE_CLUSTER_NAME,
        trigger_rule="all_done",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_success",
    )

    cluster_create >> start_pod >> cluster_delete >> end
    start_pod >> end
