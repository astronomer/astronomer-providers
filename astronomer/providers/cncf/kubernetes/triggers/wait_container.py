import asyncio
import traceback
import warnings
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
from pendulum import DateTime

from astronomer.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHookAsync


class PodLaunchTimeoutException(AirflowException):
    """When pod does not leave the ``Pending`` phase within specified timeout."""


class WaitContainerTrigger(BaseTrigger):
    """
    This class is deprecated and will be removed in 2.0.0.
    Use :class: `~airflow.providers.cncf.kubernetes.triggers.pod.KubernetesPodTrigger` instead
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
        logging_interval: Optional[int] = None,
        last_log_time: Optional[DateTime] = None,
    ):
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use `airflow.providers.cncf.kubernetes.triggers.pod.KubernetesPodTrigger`"
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__()
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook_params = hook_params
        self.container_name = container_name
        self.pod_name = pod_name
        self.pod_namespace = pod_namespace
        self.pending_phase_timeout = pending_phase_timeout
        self.poll_interval = poll_interval
        self.logging_interval = logging_interval
        self.last_log_time = last_log_time

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
                "logging_interval": self.logging_interval,
                "last_log_time": self.last_log_time,
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

    async def wait_for_container_completion(self, v1_api: CoreV1Api) -> "TriggerEvent":
        """
        Waits until container ``self.container_name`` is no longer in running state.
        If trigger is configured with a logging period, then will emit an event to
        resume the task for the purpose of fetching more logs.
        """
        time_begin = timezone.utcnow()
        time_get_more_logs = None
        if self.logging_interval is not None:
            time_get_more_logs = time_begin + timedelta(seconds=self.logging_interval)
        while True:
            pod = await v1_api.read_namespaced_pod(self.pod_name, self.pod_namespace)
            if not container_is_running(pod=pod, container_name=self.container_name):
                return TriggerEvent(
                    {"status": "done", "namespace": self.pod_namespace, "pod_name": self.pod_name}
                )
            if time_get_more_logs and timezone.utcnow() > time_get_more_logs:
                return TriggerEvent({"status": "running", "last_log_time": self.last_log_time})
            await asyncio.sleep(self.poll_interval)

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # noqa: D102
        self.log.debug("Checking pod %r in namespace %r.", self.pod_name, self.pod_namespace)
        try:
            hook = await self.get_hook()
            async with await hook.get_api_client_async() as api_client:
                v1_api = CoreV1Api(api_client)
                state = await self.wait_for_pod_start(v1_api)
                if state in PodPhase.terminal_states:
                    event = TriggerEvent(
                        {"status": "done", "namespace": self.pod_namespace, "pod_name": self.pod_name}
                    )
                else:
                    event = await self.wait_for_container_completion(v1_api)
            yield event
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
