import asyncio
import traceback
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodPhase,
    container_is_running,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone
from kubernetes_asyncio.client import CoreV1Api

from astronomer.providers.cncf.kubernetes.hooks.kubernetes_async import (
    KubernetesHookAsync,
)


class PodLaunchTimeoutException(AirflowException):
    """When pod does not leave the ``Pending`` phase within specified timeout."""


class WaitContainerTrigger(BaseTrigger):
    """
    First, waits for pod ``pod_name`` to reach running state within ``pending_phase_timeout``.
    Next, waits for ``container_name`` to reach a terminal state.

    :param kubernetes_conn_id: Airflow connection ID to use
    :param hook_params: kwargs for hook
    :param container_name: container to wait for
    :param pod_name: name of pod to monitor
    :param pod_namespace: pod namespace
    :param pending_phase_timeout: max time in seconds to wait for pod to leave pending phase
    :param poll_interval: number of seconds between reading pod state

    """

    def __init__(
        self,
        *,
        container_name: str,
        pod_name: str,
        pod_namespace: str,
        kubernetes_conn_id: Optional[str] = None,
        hook_params: Optional[Dict[str, Any]] = None,
        pending_phase_timeout: float = 120,
        poll_interval: float = 5,
    ):
        super().__init__()
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook_params = hook_params
        self.container_name = container_name
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.pending_phase_timeout = pending_phase_timeout
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:  # noqa: D102
        return (
            "astronomer.providers.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger",
            {
                "kubernetes_conn_id": self.kubernetes_conn_id,
                "hook_params": self.hook_params,
                "pod_name": self.pod_name,
                "container_name": self.container_name,
                "pod_namespace": self.pod_namespace,
                "pending_phase_timeout": self.pending_phase_timeout,
                "poll_interval": self.poll_interval,
            },
        )

    async def get_hook(self) -> KubernetesHookAsync:  # noqa: D102
        return KubernetesHookAsync(conn_id=self.kubernetes_conn_id, **(self.hook_params or {}))

    async def wait_for_pod_start(self, v1_api: CoreV1Api) -> Any:
        """
        Loops until pod phase leaves ``PENDING``
        If timeout is reached, throws error.
        """
        start_time = timezone.utcnow()
        timeout_end = start_time + timedelta(seconds=self.pending_phase_timeout)
        while timeout_end > timezone.utcnow():
            pod = await v1_api.read_namespaced_pod(self.pod_name, self.pod_namespace)
            if not pod.status.phase == "Pending":
                return pod.status.phase
            await asyncio.sleep(self.poll_interval)
        raise PodLaunchTimeoutException("Pod did not leave 'Pending' phase within specified timeout")

    async def wait_for_container_completion(self, v1_api: CoreV1Api) -> None:
        """Waits until container ``self.container_name`` is no longer in running state."""
        while True:
            pod = await v1_api.read_namespaced_pod(self.pod_name, self.pod_namespace)
            if not container_is_running(pod=pod, container_name=self.container_name):
                break
            await asyncio.sleep(self.poll_interval)

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]  # noqa: D102
        self.log.debug("Checking pod %r in namespace %r.", self.pod_name, self.pod_namespace)
        try:
            hook = await self.get_hook()
            async with await hook.get_api_client_async() as api_client:
                v1_api = CoreV1Api(api_client)
                state = await self.wait_for_pod_start(v1_api)
                if state not in PodPhase.terminal_states:
                    await self.wait_for_container_completion(v1_api)
            yield TriggerEvent({"status": "done"})
        except Exception as e:
            description = self._format_exception_description(e)
            yield TriggerEvent(
                {
                    "status": "error",
                    "error_type": e.__class__.__name__,
                    "description": description,
                }
            )

    def _format_exception_description(self, exc: Exception) -> Any:
        if isinstance(exc, PodLaunchTimeoutException):
            return exc.args[0]

        description = f"Trigger {self.__class__.__name__} failed with exception {exc.__class__.__name__}."
        message = exc.args and exc.args[0] or ""
        if message:
            description += f"\ntrigger exception message: {message}"
        curr_traceback = traceback.format_exc()
        description += f"\ntrigger traceback:\n{curr_traceback}"
        return description
