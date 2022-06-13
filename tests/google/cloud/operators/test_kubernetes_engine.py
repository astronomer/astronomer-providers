from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from kubernetes.client import models as k8s
from kubernetes.client.models.v1_object_meta import V1ObjectMeta

from astronomer.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperatorAsync,
)
from astronomer.providers.google.cloud.triggers.kubernetes_engine import (
    GKEStartPodTrigger,
)

PROJECT_ID = "astronomer-***-providers"
LOCATION = "us-west1"
GKE_CLUSTER_NAME = "provider-***-gke-cluster"
NAMESPACE = "default"
POD_NAME = "astro-k8s-gke-test-pod-25131a0d9cda46419099ac4aa8a4ef8f"
GCP_CONN_ID = "google_cloud_default"


@pytest.fixture()
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


# TODO: Improve test
@mock.patch(
    "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.get_gke_config_file"
)
@mock.patch(
    "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.build_pod_request_obj"
)
@mock.patch(
    "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.get_or_create_pod"
)
def test__get_or_create_pod(mock_get_or_create_pod, moc_build_pod_request_obj, mock_tmp):
    """assert that _get_or_create_pod does not return any value"""
    my_tmp = mock_tmp.__enter__()
    my_tmp.return_value = "/tmp/tmps90l"
    moc_build_pod_request_obj.return_value = {}
    mock_get_or_create_pod.return_value = k8s.V1Pod(metadata=V1ObjectMeta(name=POD_NAME, namespace=NAMESPACE))
    operator = GKEStartPodOperatorAsync(
        task_id="start_pod",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_name=GKE_CLUSTER_NAME,
        name="astro_k8s_gke_test_pod",
        namespace=NAMESPACE,
        image="ubuntu",
        gcp_conn_id=GCP_CONN_ID,
    )
    assert operator._get_or_create_pod(context=context) is None


@mock.patch(
    "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync._get_or_create_pod"
)
def test_execute(mock__get_or_create_pod):
    """
    asserts that a task is deferred and a GKEStartPodTrigger will be fired
    when the GKEStartPodOperatorAsync is executed.
    """
    mock__get_or_create_pod.return_value = None
    operator = GKEStartPodOperatorAsync(
        task_id="start_pod",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_name=GKE_CLUSTER_NAME,
        name="astro_k8s_gke_test_pod",
        namespace=NAMESPACE,
        image="ubuntu",
        gcp_conn_id=GCP_CONN_ID,
    )
    with pytest.raises(TaskDeferred) as exc:
        operator.execute(context)
    assert isinstance(exc.value.trigger, GKEStartPodTrigger), "Trigger is not a GKEStartPodTrigger"


def test_execute_complete_success():
    """assert that execute_complete_success log correct message when a task succeed"""
    operator = GKEStartPodOperatorAsync(
        task_id="start_pod",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_name=GKE_CLUSTER_NAME,
        name="astro_k8s_gke_test_pod",
        namespace=NAMESPACE,
        image="ubuntu",
        gcp_conn_id=GCP_CONN_ID,
    )
    with mock.patch.object(operator.log, "info") as mock_log_info:
        operator.execute_complete(context=context, event={"status": "done"})
    mock_log_info.assert_called_with("Job completed successfully")


def test_execute_complete_fail():
    operator = GKEStartPodOperatorAsync(
        task_id="start_pod",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_name=GKE_CLUSTER_NAME,
        name="astro_k8s_gke_test_pod",
        namespace=NAMESPACE,
        image="ubuntu",
        gcp_conn_id=GCP_CONN_ID,
    )
    with pytest.raises(AirflowException):
        """assert that execute_complete_success raise exception when a task fail"""
        operator.execute_complete(context=context, event={"status": "error", "description": "Pod not found"})
