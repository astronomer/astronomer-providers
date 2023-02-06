from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodLoggingStatus
from kubernetes.client import models as k8s
from kubernetes.client.models.v1_object_meta import V1ObjectMeta

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    PodNotFoundException,
)
from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    PodLaunchTimeoutException,
)
from astronomer.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperatorAsync,
)
from astronomer.providers.google.cloud.triggers.kubernetes_engine import (
    GKEStartPodTrigger,
)
from tests.utils.airflow_util import create_context

PROJECT_ID = "astronomer-***-providers"
LOCATION = "us-west1"
GKE_CLUSTER_NAME = "provider-***-gke-cluster"
NAMESPACE = "default"
POD_NAME = "astro-k8s-gke-test-pod-25131a0d9cda46419099ac4aa8a4ef8f"
GCP_CONN_ID = "google_cloud_default"


# TODO: Improve test
class TestGKEStartPodOperatorAsync:
    OPERATOR = GKEStartPodOperatorAsync(
        task_id="start_pod",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_name=GKE_CLUSTER_NAME,
        name="astro_k8s_gke_test_pod",
        namespace=NAMESPACE,
        image="ubuntu",
        gcp_conn_id=GCP_CONN_ID,
    )
    OPERATOR1 = GKEStartPodOperatorAsync(
        task_id="start_pod",
        project_id=PROJECT_ID,
        location=LOCATION,
        cluster_name=GKE_CLUSTER_NAME,
        name="astro_k8s_gke_test_pod",
        namespace=NAMESPACE,
        image="ubuntu",
        gcp_conn_id=GCP_CONN_ID,
        get_logs=True,
    )

    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.get_gke_config_file"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.build_pod_request_obj"
    )
    @mock.patch(
        "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.get_or_create_pod"
    )
    def test__get_or_create_pod(self, mock_get_or_create_pod, moc_build_pod_request_obj, mock_tmp, context):
        """assert that _get_or_create_pod does not return any value"""
        my_tmp = mock_tmp.__enter__()
        my_tmp.return_value = "/tmp/tmps90l"
        moc_build_pod_request_obj.return_value = {}
        mock_get_or_create_pod.return_value = k8s.V1Pod(
            metadata=V1ObjectMeta(name=POD_NAME, namespace=NAMESPACE)
        )

        assert self.OPERATOR._get_or_create_pod(context=context) is None

    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync._get_or_create_pod"
    )
    def test_execute(self, mock__get_or_create_pod, context):
        """
        asserts that a task is deferred and a GKEStartPodTrigger will be fired
        when the GKEStartPodOperatorAsync is executed.
        """
        mock__get_or_create_pod.return_value = None

        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(context)
        assert isinstance(exc.value.trigger, GKEStartPodTrigger), "Trigger is not a GKEStartPodTrigger"

    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync.trigger_reentry"
    )
    def test_execute_complete_success(self, mock_trigger_reentry):
        """assert that execute_complete_success log correct message when a task succeed"""
        mock_trigger_reentry.return_value = {}

        assert self.OPERATOR.execute_complete(context=create_context(self.OPERATOR), event={}) is None

    def test_execute_complete_fail(self, context):
        with pytest.raises(AirflowException):
            """assert that execute_complete_success raise exception when a task fail"""
            self.OPERATOR.execute_complete(
                context=context, event={"status": "error", "description": "Pod not found"}
            )

    def test_raise_for_trigger_status_done(self):
        """Assert trigger don't raise exception in case of status is done"""
        assert self.OPERATOR.raise_for_trigger_status({"status": "done"}) is None

    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.client")
    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync.cleanup"
    )
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync"
        ".raise_for_trigger_status"
    )
    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync.find_pod"
    )
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook._get_default_client")
    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.get_gke_config_file"
    )
    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync.extract_xcom"
    )
    def test_get_logs_not_running(
        self,
        mock_extract_xcom,
        mock_gke_config,
        mock_get_default_client,
        fetch_container_logs,
        await_pod_completion,
        find_pod,
        raise_for_trigger_status,
        get_kube_client,
        cleanup,
        mock_client,
    ):
        mock_extract_xcom.return_value = "{}"
        pod = MagicMock()
        find_pod.return_value = pod
        mock_client.return_value = {}

        context = create_context(self.OPERATOR1)
        await_pod_completion.return_value = None
        fetch_container_logs.return_value = PodLoggingStatus(False, None)
        self.OPERATOR1.trigger_reentry(context, {"namespace": NAMESPACE})
        fetch_container_logs.is_called_with(pod, "base")

    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.client")
    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync.cleanup"
    )
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync"
        ".raise_for_trigger_status"
    )
    @mock.patch(
        "astronomer.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperatorAsync.find_pod"
    )
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook._get_default_client")
    @mock.patch(
        "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator.get_gke_config_file"
    )
    def test_no_pod(
        self,
        mock_gke_config,
        mock_get_default_client,
        fetch_container_logs,
        await_pod_completion,
        find_pod,
        raise_for_trigger_status,
        get_kube_client,
        cleanup,
        mock_client,
    ):
        """Assert if pod not found then raise exception"""
        find_pod.return_value = None

        context = create_context(self.OPERATOR1)
        with pytest.raises(PodNotFoundException):
            self.OPERATOR1.trigger_reentry(context, {"namespace": NAMESPACE})

    def test_trigger_error(self, context):
        """Assert that trigger_reentry raise exception in case of error"""

        with pytest.raises(PodLaunchTimeoutException):
            self.OPERATOR1.execute_complete(
                context,
                {
                    "status": "error",
                    "error_type": "PodLaunchTimeoutException",
                    "description": "any message",
                },
            )
