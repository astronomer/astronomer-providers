import asyncio
from datetime import timedelta
from typing import Any, Dict, Optional, Tuple

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.pod_manager import container_is_running
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone
from kubernetes_asyncio import client

from astronomer_operators.cncf.kubernetes.hooks.kubernetes_async import (
    KubernetesHookAsync,
)


class PodLaunchTimeoutException(AirflowException):
    """When pod does not leave the ``Pending`` phase within specified timeout."""


class WaitContainerTrigger(BaseTrigger):
    """
    First, waits for pod ``pod_name`` to reach running state within ``pending_phase_timeout``.
    Next, waits for ``container_name`` to reach a terminal state.

    :param kubernetes_conn_id: Airflow connection ID to use
    :type kubernetes_conn_id: str
    :param hook_params: kwargs for hook
    :type hook_params: dict
    :param container_name: container to wait for
    :type container_name: str
    :param pod_name: name of pod to monitor
    :type pod_name: str
    :param pod_namespace: pod namespace
    :type pod_namespace: str
    :param pending_phase_timeout: max time in seconds to wait for pod to leave pending phase
    :type pending_phase_timeout: int
    :param poll_interval: number of seconds between reading pod state
    :type poll_interval: int

    """

    TERMINAL_STATES = {"Succeeded", "Failed"}

    def __init__(
        self,
        kubernetes_conn_id: Optional[str] = None,
        hook_params: Optional[dict] = None,
        container_name: Optional[str] = None,
        pod_name: Optional[str] = None,
        pod_namespace: Optional[str] = None,
        pending_phase_timeout: Optional[int] = 120,
        poll_interval: Optional[int] = 5,
    ):
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook_params = hook_params
        self.container_name = container_name
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.pending_phase_timeout = pending_phase_timeout
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "astronomer_operators.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger",
            dict(
                kubernetes_conn_id=self.kubernetes_conn_id,
                hook_params=self.hook_params,
                pod_name=self.pod_name,
                container_name=self.container_name,
                pod_namespace=self.pod_namespace,
                pending_phase_timeout=self.pending_phase_timeout,
                poll_interval=self.poll_interval,
            ),
        )

    async def get_hook(self) -> KubernetesHookAsync:
        return KubernetesHookAsync(conn_id=self.kubernetes_conn_id, **(self.hook_params or {}))

    async def wait_for_pod_start(self, v1):
        """
        Loops until pod phase leaves ``PENDING``
        If timeout is reached, throws error.
        """
        start_time = timezone.utcnow()
        timeout_end = start_time + timedelta(seconds=self.pending_phase_timeout)
        while timeout_end > timezone.utcnow():
            pod = await v1.read_namespaced_pod(self.pod_name, self.pod_namespace)
            if not pod.status.phase == "Pending":
                return pod.status.phase
            await asyncio.sleep(self.poll_interval)
        raise PodLaunchTimeoutException("Pod did not leave 'Pending' phase within specified timeout")

    async def wait_for_container_completion(self, v1):
        """Waits until container ``self.container_name`` is no longer in running state."""
        while True:
            pod = await v1.read_namespaced_pod(self.pod_name, self.pod_namespace)
            if not container_is_running(pod=pod, container_name=self.container_name):
                break
            await asyncio.sleep(self.poll_interval)

    async def run(self):
        self.log.debug(f"checking {self.pod_name=}, {self.pod_namespace=}")
        try:
            hook = await self.get_hook()
            async with await hook.get_api_client_async() as api:
                v1 = client.CoreV1Api(api)
                state = await self.wait_for_pod_start(v1)
                if state not in self.TERMINAL_STATES:
                    await self.wait_for_container_completion(v1)
            yield TriggerEvent({"status": "done"})
        except PodLaunchTimeoutException as e:
            message = e.args and e.args[0] or ""
            yield TriggerEvent(
                {
                    "status": "error",
                    "error_type": "PodLaunchTimeoutException",
                    "description": message,
                }
            )
