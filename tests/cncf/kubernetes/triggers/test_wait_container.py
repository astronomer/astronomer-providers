import re
from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow.triggers.base import TriggerEvent

from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    WaitContainerTrigger,
)

TRIGGER_CLASS = "astronomer.providers.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger"
READ_NAMESPACED_POD_PATH = "kubernetes_asyncio.client.CoreV1Api.read_namespaced_pod"


def test_serialize():
    """
    Asserts that the Trigger correctly serializes its arguments
    and classpath.
    """
    expected_kwargs = {
        "kubernetes_conn_id": None,
        "hook_params": {
            "cluster_context": "cluster_context",
            "config_file": "config_file",
            "in_cluster": "in_cluster",
        },
        "pod_name": "pod_name",
        "container_name": "container_name",
        "pod_namespace": "pod_namespace",
        "pending_phase_timeout": 120,
        "poll_interval": 5,
        "logging_interval": None,
        "last_log_time": None,
    }
    trigger = WaitContainerTrigger(**expected_kwargs)
    classpath, actual_kwargs = trigger.serialize()
    assert classpath == TRIGGER_CLASS
    assert actual_kwargs == expected_kwargs


def get_read_pod_mock(phases_to_emit=None):
    async def mock_read_namespaced_pod(*args, **kwargs):
        event_mock = MagicMock()
        event_mock.status.phase = phases_to_emit.pop(0)
        return event_mock

    return mock_read_namespaced_pod


@pytest.mark.asyncio
@mock.patch(READ_NAMESPACED_POD_PATH, new=get_read_pod_mock(["Pending", "Pending", "Pending", "Pending"]))
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_pending_timeout(load_kube_config):
    """Verify that PodLaunchTimeoutException is yielded when timeout reached"""
    trigger = WaitContainerTrigger(
        pod_name=mock.ANY,
        pod_namespace=mock.ANY,
        container_name=mock.ANY,
        pending_phase_timeout=5,
        poll_interval=2,
    )

    assert await trigger.run().__anext__() == TriggerEvent(
        {
            "status": "error",
            "error_type": "PodLaunchTimeoutException",
            "description": "Pod did not leave 'Pending' phase within specified timeout",
        }
    )


@pytest.mark.asyncio
@mock.patch(READ_NAMESPACED_POD_PATH)
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_other_exception(load_kube_config, read_mock):
    """Verify that any exception is emitted as an event"""
    read_mock.side_effect = [NotImplementedError("testing")]
    trigger = WaitContainerTrigger(
        pod_name=mock.ANY,
        pod_namespace=mock.ANY,
        container_name=mock.ANY,
        pending_phase_timeout=5,
        poll_interval=2,
    )
    message = "\n".join(
        [
            r"Trigger WaitContainerTrigger failed with exception NotImplementedError.",
            r"trigger exception message: testing",
            r"trigger traceback:",
            r"Traceback \(most recent call last\):",
            r'  File ".+cncf\/kubernetes\/triggers\/wait_container.py\", line .+',
        ]
    )
    actual = await trigger.run().__anext__()
    assert isinstance(actual, TriggerEvent)
    assert actual.payload["status"] == "error"
    assert actual.payload["error_type"] == "NotImplementedError"
    description = actual.payload["description"]
    assert re.match(message, description) is not None


@pytest.mark.asyncio
@mock.patch(READ_NAMESPACED_POD_PATH, new=get_read_pod_mock(["Pending", "Succeeded"]))
@mock.patch(f"{TRIGGER_CLASS}.wait_for_container_completion")
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_pending_succeeded(load_kube_config, wait_completion):
    """
    When we get pod phase Succeeded we should immediately emit done event
    and not call wait for pod completion.
    """
    trigger = WaitContainerTrigger(
        pod_name=mock.ANY,
        pod_namespace=mock.ANY,
        container_name=mock.ANY,
        pending_phase_timeout=5,
        poll_interval=2,
    )

    assert await trigger.run().__anext__() == TriggerEvent({"status": "done"})
    wait_completion.assert_not_awaited()


@pytest.mark.asyncio
@mock.patch(READ_NAMESPACED_POD_PATH, new=get_read_pod_mock(["Pending", "Running"]))
@mock.patch(f"{TRIGGER_CLASS}.wait_for_container_completion")
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_pending_running(load_kube_config, wait_completion):
    """
    If we get Running phase within the timeout period we should move on to wait
    for pod completion.
    """
    wait_completion.return_value = None
    trigger = WaitContainerTrigger(
        pod_name=mock.ANY,
        pod_namespace=mock.ANY,
        container_name=mock.ANY,
        pending_phase_timeout=5,
        poll_interval=2,
        logging_interval=None,
    )

    assert await trigger.run().__anext__() == TriggerEvent({"status": "done"})
    wait_completion.assert_awaited()


@pytest.mark.asyncio
@mock.patch(READ_NAMESPACED_POD_PATH, new=get_read_pod_mock(["Failed"]))
@mock.patch(f"{TRIGGER_CLASS}.wait_for_container_completion")
@mock.patch("kubernetes_asyncio.config.load_kube_config")
async def test_failed(load_kube_config, wait_completion):
    """
    When pod goes straight to 'Failed' phase during 'wait pod start'
    we should immediately send execution back to KPO.  This is not a
    trigger error and KPO will detect and handle the pod failure.
    """
    trigger = WaitContainerTrigger(
        pod_name=mock.ANY,
        pod_namespace=mock.ANY,
        container_name=mock.ANY,
        pending_phase_timeout=5,
        poll_interval=2,
    )

    assert await trigger.run().__anext__() == TriggerEvent({"status": "done"})
    wait_completion.assert_not_awaited()
