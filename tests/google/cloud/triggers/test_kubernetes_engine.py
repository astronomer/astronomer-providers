from unittest import mock

import pytest
from airflow.triggers.base import TriggerEvent
from kubernetes_asyncio import client

from astronomer.providers.google.cloud.triggers.kubernetes_engine import GKEStartPodTrigger

PROJECT_ID = "astronomer-***-providers"
LOCATION = "us-west1"
NAMESPACE = "default"
GCP_CONN_ID = "google_cloud_default"
POD_NAME = "astro-k8s-gke-test-pod-25131a0d9cda46419099ac4aa8a4ef8f"
MODULE_GOOGLE = "airflow.providers.google.cloud"
MODULE_CNCF = "astronomer.providers.cncf"


class TestGKEStartPodTrigger:
    TRIGGER = GKEStartPodTrigger(
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

    def test_serialization(self):
        """asserts that the GKEStartPodTrigger correctly serializes its argument and classpath."""

        classpath, kwargs = self.TRIGGER.serialize()
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "mock_state, expected_value",
        [
            ("Succeeded", {"status": "done", "namespace": NAMESPACE, "pod_name": POD_NAME}),
            (
                "Failed",
                {
                    "status": "failed",
                    "namespace": NAMESPACE,
                    "pod_name": POD_NAME,
                    "description": "Failed to start pod operator",
                },
            ),
        ],
    )
    @mock.patch(f"{MODULE_CNCF}.kubernetes.triggers.wait_container.WaitContainerTrigger.wait_for_pod_start")
    @mock.patch(
        "astronomer.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHookAsync.get_api_client_async"
    )
    @mock.patch("astronomer.providers.google.cloud.triggers.kubernetes_engine._get_gke_config_file")
    async def test_run(self, mock_tmp, get_api_client_async, wait_for_pod_start, mock_state, expected_value):
        """assert that when wait_for_pod_start succeeded run method yield correct event"""
        my_tmp = mock_tmp.__enter__()
        my_tmp.return_value = "/tmp/tmps90l"
        get_api_client_async.return_value = client.ApiClient()
        wait_for_pod_start.return_value = mock_state

        assert await self.TRIGGER.run().__anext__() == TriggerEvent(expected_value)

    @pytest.mark.asyncio
    @mock.patch(
        f"{MODULE_CNCF}.kubernetes.triggers.wait_container.WaitContainerTrigger.wait_for_container_completion"
    )
    @mock.patch(f"{MODULE_CNCF}.kubernetes.triggers.wait_container.WaitContainerTrigger.wait_for_pod_start")
    @mock.patch(
        "astronomer.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHookAsync.get_api_client_async"
    )
    @mock.patch("astronomer.providers.google.cloud.triggers.kubernetes_engine._get_gke_config_file")
    async def test_run_pending(
        self, mock_tmp, get_api_client_async, wait_for_pod_start, wait_for_container_completion
    ):
        """assert that when wait_for_pod_start Pending run method yield wait_for_container_completion response"""
        my_tmp = mock_tmp.__enter__()
        my_tmp.return_value = "/tmp/tmps90l"
        get_api_client_async.return_value = client.ApiClient()
        wait_for_pod_start.return_value = "Pending"
        wait_for_container_completion.return_value = TriggerEvent({"status": "done"})

        assert await self.TRIGGER.run().__anext__() == TriggerEvent({"status": "done"})

    @pytest.mark.asyncio
    @mock.patch("astronomer.providers.google.cloud.triggers.kubernetes_engine._get_gke_config_file")
    async def test_run_exception(self, mock_tmp):
        """assert that run raise exception when fail to fetch GKE kube config file"""
        my_tmp = mock_tmp.__enter__()
        my_tmp.return_value = None

        assert await self.TRIGGER.run().__anext__() == TriggerEvent(
            {
                "status": "error",
                "message": "Invalid kube-config file. Expected key current-context in kube-config",
            }
        )
