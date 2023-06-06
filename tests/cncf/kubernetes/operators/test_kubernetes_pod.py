from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import TaskDeferred
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodLoggingStatus,
    PodPhase,
)
from kubernetes.client import models as k8s

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperatorAsync,
    PodNotFoundException,
)
from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    PodLaunchTimeoutException,
)
from tests.utils.airflow_util import create_context

KUBE_POD_MOD = "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod"


def _build_mock_pod(state: k8s.V1ContainerState) -> k8s.V1Pod:
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="base", namespace="default"),
        status=k8s.V1PodStatus(
            container_statuses=[
                k8s.V1ContainerStatus(
                    name="base",
                    image="alpine",
                    image_id="1",
                    ready=True,
                    restart_count=1,
                    state=state,
                )
            ]
        ),
    )


class TestKubernetesPodOperatorAsync:
    def test_raise_for_trigger_status_pending_timeout(self):
        """Assert trigger raise exception in case of timeout"""
        with pytest.raises(PodLaunchTimeoutException):
            KubernetesPodOperatorAsync.raise_for_trigger_status(
                {
                    "status": "error",
                    "error_type": "PodLaunchTimeoutException",
                    "description": "any message",
                }
            )

    def test_raise_for_trigger_status_done(self):
        """Assert trigger don't raise exception in case of status is done"""
        assert KubernetesPodOperatorAsync.raise_for_trigger_status({"status": "done"}) is None

    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.client")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.cleanup")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.raise_for_trigger_status")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.find_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook._get_default_client")
    def test_get_logs_running(
        self,
        mock_get_default_client,
        fetch_container_logs,
        await_pod_completion,
        find_pod,
        raise_for_trigger_status,
        get_kube_client,
        cleanup,
        mock_client,
    ):
        """When logs fetch exits with status running, raise task deferred"""
        pod = MagicMock()
        find_pod.return_value = pod
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        mock_client.return_value = {}
        context = create_context(op)
        await_pod_completion.return_value = None
        fetch_container_logs.return_value = PodLoggingStatus(True, None)
        with pytest.raises(TaskDeferred):
            op.trigger_reentry(context, None)
        fetch_container_logs.is_called_with(pod, "base")

    @mock.patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.client")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.cleanup")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.raise_for_trigger_status")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.find_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook._get_default_client")
    def test_get_logs_not_running(
        self,
        mock_get_default_client,
        fetch_container_logs,
        await_pod_completion,
        find_pod,
        raise_for_trigger_status,
        get_kube_client,
        cleanup,
        mock_client,
    ):
        pod = MagicMock()
        find_pod.return_value = pod
        mock_client.return_value = {}
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        context = create_context(op)
        await_pod_completion.return_value = None
        fetch_container_logs.return_value = PodLoggingStatus(False, None)
        op.trigger_reentry(context, None)
        fetch_container_logs.is_called_with(pod, "base")

    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.cleanup")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.raise_for_trigger_status")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.find_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook._get_default_client")
    def test_no_pod(
        self,
        mock_get_default_client,
        fetch_container_logs,
        await_pod_completion,
        find_pod,
        raise_for_trigger_status,
        get_kube_client,
        cleanup,
    ):
        """Assert if pod not found then raise exception"""
        find_pod.return_value = None
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        context = create_context(op)
        with pytest.raises(PodNotFoundException):
            op.trigger_reentry(context, None)

    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.cleanup")
    @mock.patch("airflow.kubernetes.kube_client.get_kube_client")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.find_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_container_logs")
    @mock.patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook._get_default_client")
    def test_trigger_error(
        self,
        mock_get_default_client,
        fetch_container_logs,
        await_pod_completion,
        find_pod,
        get_kube_client,
        cleanup,
    ):
        """Assert that trigger_reentry raise exception in case of error"""
        find_pod.return_value = MagicMock()
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        with pytest.raises(PodLaunchTimeoutException):
            context = create_context(op)
            op.trigger_reentry(
                context,
                {
                    "status": "error",
                    "error_type": "PodLaunchTimeoutException",
                    "description": "any message",
                },
            )

    def test_defer_with_kwargs(self):
        """Assert that with kwargs throw exception"""
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        with pytest.raises(ValueError):
            op.defer(kwargs={"timeout": 10})

    @pytest.mark.parametrize("pod_phase", PodPhase.terminal_states)
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.trigger_reentry")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.build_pod_request_obj")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.get_or_create_pod")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.defer")
    def test_execute_done_before_defer(
        self, mock_defer, mock_get_or_create_pod, mock_build_pod_request_obj, mock_trigger_reentry, pod_phase
    ):
        mock_get_or_create_pod.return_value.status.phase = pod_phase
        mock_build_pod_request_obj.return_value = {}
        mock_defer.return_value = {}
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        assert op.execute(context=create_context(op))
        assert mock_trigger_reentry.called
        assert not mock_defer.called

    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.build_pod_request_obj")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.get_or_create_pod")
    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.defer")
    def test_execute(
        self,
        mock_defer,
        mock_get_or_create_pod,
        mock_build_pod_request_obj,
    ):
        """Assert that execute succeeded"""
        mock_get_or_create_pod.return_value = _build_mock_pod(
            k8s.V1ContainerState(
                {"running": k8s.V1ContainerStateRunning(), "terminated": None, "waiting": None}
            )
        )
        mock_build_pod_request_obj.return_value = {}
        mock_defer.return_value = {}
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        assert op.execute(context=create_context(op)) is None

    @mock.patch(f"{KUBE_POD_MOD}.KubernetesPodOperatorAsync.trigger_reentry")
    def test_execute_complete(self, mock_trigger_reentry):
        """Assert that execute_complete succeeded"""
        mock_trigger_reentry.return_value = {}
        op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
        assert op.execute_complete(context=create_context(op), event={}) is None
