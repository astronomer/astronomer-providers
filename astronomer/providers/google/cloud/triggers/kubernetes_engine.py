import asyncio
from datetime import timedelta
from typing import Any, AsyncIterator, Dict, Optional, Sequence, Tuple, Union

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.utils.pod_manager import (
    PodPhase,
    container_is_running,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils import timezone
from kubernetes_asyncio.client import CoreV1Api

from astronomer.providers.cncf.kubernetes.hooks.kubernetes_async import (
    KubernetesHookAsync,
)


class PodLaunchTimeoutException(AirflowException):
    """When pod does not leave the ``Pending`` phase within specified timeout."""


class GKEStartPodTrigger(BaseTrigger):
    """
    Fetch GKE cluster config and wait for pod to start up.

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides
    :param cluster_name: The name of the Google Kubernetes Engine cluster the pod should be spawned in
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The google cloud connection id to use
    :param impersonation_chain: Optional service account to impersonate using short-term credentials
    :param is_delete_operator_pod: What to do when the pod reaches its final state
    :param regional: The location param is region name
    :param namespace: The cluster namespace
    :param name: The pod name
    :param cluster_context: Context that points to kubernetes cluster
    :param in_cluster: run kubernetes client with in_cluster configuration
    :param pending_phase_timeout: max time in seconds to wait for pod to leave pending phase
    """

    BASE_CONTAINER_NAME = "base"
    POD_CHECKED_KEY = "already_checked"

    def __init__(
        self,
        *,
        location: str,
        cluster_name: str = "default",
        use_internal_ip: bool = False,
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        is_delete_operator_pod: bool = True,
        regional: bool = False,
        namespace: Optional[str] = None,
        name: Optional[str] = None,
        cluster_context: Optional[str] = None,
        in_cluster: Optional[bool] = None,
        poll_interval: float = 5.0,
        pending_phase_timeout: float = 120.0,
    ):
        super().__init__()
        self.location = location
        self.cluster_name = cluster_name
        self.regional = regional
        self.use_internal_ip = use_internal_ip
        self.project_id = project_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.cluster_context = cluster_context
        self.in_cluster = in_cluster
        self.namespace = namespace
        self.is_delete_operator_pod = is_delete_operator_pod
        self.name = name
        self.poll_interval = poll_interval
        self.pending_phase_timeout = pending_phase_timeout

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize GKEStartPodTrigger object"""
        return (
            "astronomer.providers.google.cloud.triggers.kubernetes_engine.GKEStartPodTrigger",
            {
                "project_id": self.project_id,
                "cluster_name": self.cluster_name,
                "impersonation_chain": self.impersonation_chain,
                "regional": self.regional,
                "location": self.location,
                "use_internal_ip": self.use_internal_ip,
                "gcp_conn_id": self.gcp_conn_id,
                "is_delete_operator_pod": self.is_delete_operator_pod,
                "name": self.name,
                "namespace": self.namespace,
                "poll_interval": self.poll_interval,
                "pending_phase_timeout": self.pending_phase_timeout,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Wait for pod to reach terminal state"""
        with GKEStartPodOperator.get_gke_config_file(
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            impersonation_chain=self.impersonation_chain,
            regional=self.regional,
            location=self.location,
            use_internal_ip=self.use_internal_ip,
        ) as config_file:
            hook_params: Optional[Dict[str, Any]] = {
                "cluster_context": self.cluster_context,
                "config_file": config_file,
                "in_cluster": self.in_cluster,
            }
            hook = KubernetesHookAsync(conn_id=None, **(hook_params or {}))

            async with await hook.get_api_client_async() as api_client:
                v1_api = CoreV1Api(api_client)
                state = await self.wait_for_pod_start(v1_api)
                if state in PodPhase.terminal_states:
                    event = TriggerEvent({"status": "done"})
                else:
                    event = await self.wait_for_container_completion(v1_api)
            yield event

    async def wait_for_pod_start(self, v1_api: CoreV1Api) -> Any:
        """
        Loops until pod phase leaves ``PENDING``
        If timeout is reached, throws error.
        """
        start_time = timezone.utcnow()
        timeout_end = start_time + timedelta(seconds=self.pending_phase_timeout)
        while timeout_end > timezone.utcnow():
            pod = await v1_api.read_namespaced_pod(self.name, self.namespace)
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
        while True:
            pod = await v1_api.read_namespaced_pod(self.name, self.namespace)
            if not container_is_running(pod=pod, container_name=self.BASE_CONTAINER_NAME):
                return TriggerEvent({"status": "done"})
            await asyncio.sleep(5)
