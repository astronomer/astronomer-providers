from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator

from astronomer.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperatorAsync

PROJECT_ID = "astronomer-***-providers"
LOCATION = "us-west1"
GKE_CLUSTER_NAME = "provider-***-gke-cluster"
NAMESPACE = "default"
POD_NAME = "astro-k8s-gke-test-pod-25131a0d9cda46419099ac4aa8a4ef8f"
GCP_CONN_ID = "google_cloud_default"


class TestGKEStartPodOperatorAsync:
    def test_init(self):
        task = GKEStartPodOperatorAsync(
            task_id="start_pod",
            project_id=PROJECT_ID,
            location=LOCATION,
            cluster_name=GKE_CLUSTER_NAME,
            name="astro_k8s_gke_test_pod",
            namespace=NAMESPACE,
            image="ubuntu",
            gcp_conn_id=GCP_CONN_ID,
            logging_interval=1,
        )
        assert isinstance(task, GKEStartPodOperator)
        assert task.deferrable is True
