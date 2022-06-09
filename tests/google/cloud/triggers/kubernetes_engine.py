from astronomer.providers.google.cloud.triggers.kubernetes_engine import (
    GKEStartPodTrigger,
)

PROJECT_ID = "astronomer-***-providers"
LOCATION = "us-west1"
GKE_CLUSTER_NAME = "provider-***-gke-cluster"
NAMESPACE = "default"
GCP_CONN_ID = "google_cloud_default"
POD_NAME = "astro-k8s-gke-test-pod-25131a0d9cda46419099ac4aa8a4ef8f"


def serialization():
    """Asserts that the GKEStartPodTrigger correctly serializes its argument and classpath."""
    trigger = GKEStartPodTrigger(
        namespace=NAMESPACE,
        name=POD_NAME,
        in_cluster=False,
        cluster_context=None,
        location=LOCATION,
        cluster_name="base",
        use_internal_ip=False,
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=None,
        regional=False,
        poll_interval=5,
        pending_phase_timeout=120,
        logging_interval=None,
    )

    classpath, kwargs = trigger.serialize()
    assert classpath == "astronomer.providers.google.cloud.triggers.kubernetes_engine.GKEStartPodTrigger"
    assert kwargs == {
        "location": LOCATION,
        "cluster_name": "base",
        "regional": False,
        "use_internal_ip": False,
        "project_id": PROJECT_ID,
        "gcp_conn_id": GCP_CONN_ID,
        "impersonation_chain": None,
        "cluster_context": None,
        "in_cluster": False,
        "namespace": NAMESPACE,
        "name": POD_NAME,
        "poll_interval": 5,
        "pending_phase_timeout": 120,
        "logging_interval": None,
    }
