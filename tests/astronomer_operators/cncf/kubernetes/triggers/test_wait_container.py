from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer_operators.cncf.kubernetes.triggers.wait_container import (
    WaitContainerTrigger,
)


def test_serialize():
    """
    Asserts that the Trigger correctly serializes its arguments
    and classpath.
    """
    expected_kwargs = dict(
        kubernetes_conn_id=None,
        hook_params=dict(
            cluster_context="cluster_context",
            config_file="config_file",
            in_cluster="in_cluster",
        ),
        pod_name="pod_name",
        container_name="container_name",
        pod_namespace="pod_namespace",
        pending_phase_timeout=120,
        poll_interval=5,
    )
    trigger = WaitContainerTrigger(**expected_kwargs)
    classpath, actual_kwargs = trigger.serialize()
    assert classpath == "astronomer_operators.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger"
    assert actual_kwargs == expected_kwargs


def get_read_pod_mock(states):
    async def mock_read_namespaced_pod(*args, **kwargs):
        event_mock = MagicMock()
        event_mock.status.phase = states.pop(0)
        return event_mock

    return mock_read_namespaced_pod


@pytest.mark.asyncio
@mock.patch(
    "kubernetes_asyncio.client.CoreV1Api.read_namespaced_pod",
    new=get_read_pod_mock(["Pending", "Pending", "Pending", "Pending"]),
)
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_pending_timeout(load_kube_config):
    trigger = WaitContainerTrigger(pending_phase_timeout=5, poll_interval=2)

    assert await trigger.run().__anext__() == TriggerEvent(
        {
            "status": "error",
            "error_type": "PodLaunchTimeoutException",
            "description": "Pod did not leave 'Pending' phase within specified timeout",
        }
    )


@pytest.mark.asyncio
@mock.patch(
    "kubernetes_asyncio.client.CoreV1Api.read_namespaced_pod",
    new=get_read_pod_mock(["Pending", "Succeeded"]),
)
@mock.patch(
    "astronomer_operators.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger.wait_for_container_completion"
)
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_pending_succeeded(load_kube_config, wait_completion):
    trigger = WaitContainerTrigger(pending_phase_timeout=5, poll_interval=2)

    assert await trigger.run().__anext__() == TriggerEvent({"status": "done"})
    wait_completion.assert_not_awaited()


@pytest.mark.asyncio
@mock.patch(
    "kubernetes_asyncio.client.CoreV1Api.read_namespaced_pod",
    new=get_read_pod_mock(["Pending", "Running"]),
)
@mock.patch(
    "astronomer_operators.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger.wait_for_container_completion"
)
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_pending_running(load_kube_config, wait_completion):
    trigger = WaitContainerTrigger(pending_phase_timeout=5, poll_interval=2)

    assert await trigger.run().__anext__() == TriggerEvent({"status": "done"})
    wait_completion.assert_awaited()


@pytest.mark.asyncio
@mock.patch(
    "kubernetes_asyncio.client.CoreV1Api.read_namespaced_pod",
    new=get_read_pod_mock(["Failed"]),
)
@mock.patch(
    "astronomer_operators.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger.wait_for_container_completion"
)
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_failed(load_kube_config, wait_completion):
    trigger = WaitContainerTrigger(pending_phase_timeout=5, poll_interval=2)

    assert await trigger.run().__anext__() == TriggerEvent({"status": "done"})
    wait_completion.assert_not_awaited()
