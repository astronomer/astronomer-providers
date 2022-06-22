from typing import Any, AsyncIterator, Dict, Optional, Sequence, Tuple, Union

from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.triggers.base import TriggerEvent
from kubernetes_asyncio.client import CoreV1Api

from astronomer.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHookAsync
from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    WaitContainerTrigger,
)


class GKEStartPodTrigger(WaitContainerTrigger):
    """
    Fetch GKE cluster config and wait for pod to start up.

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides
    :param cluster_name: The name of the Google Kubernetes Engine cluster the pod should be spawned in
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The google cloud connection id to use
    :param impersonation_chain: Optional service account to impersonate using short-term credentials
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
        namespace: str,
        name: str,
        cluster_name: str = "default",
        use_internal_ip: bool = False,
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        regional: bool = False,
        cluster_context: Optional[str] = None,
        in_cluster: Optional[bool] = None,
        poll_interval: float = 5.0,
        pending_phase_timeout: float = 120.0,
        logging_interval: Optional[int] = None,
    ):
        super().__init__(
            container_name=self.BASE_CONTAINER_NAME,
            pod_name=name,
            pod_namespace=namespace,
            logging_interval=logging_interval,
        )
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
        self.name = name
        self.poll_interval = poll_interval
        self.pending_phase_timeout = pending_phase_timeout
        self.logging_interval = logging_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize GKEStartPodTrigger object"""
        return (
            "astronomer.providers.google.cloud.triggers.kubernetes_engine.GKEStartPodTrigger",
            {
                "location": self.location,
                "cluster_name": self.cluster_name,
                "regional": self.regional,
                "use_internal_ip": self.use_internal_ip,
                "project_id": self.project_id,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "cluster_context": self.cluster_context,
                "in_cluster": self.in_cluster,
                "namespace": self.namespace,
                "name": self.name,
                "poll_interval": self.poll_interval,
                "pending_phase_timeout": self.pending_phase_timeout,
                "logging_interval": self.logging_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Wait for pod to reach terminal state"""
        try:
            with GKEStartPodOperator.get_gke_config_file(
                gcp_conn_id=self.gcp_conn_id,
                project_id=self.project_id,
                cluster_name=self.cluster_name,
                impersonation_chain=self.impersonation_chain,
                regional=self.regional,
                location=self.location,
                use_internal_ip=self.use_internal_ip,
            ) as config_file:
                hook_params: Dict[str, Any] = {
                    "cluster_context": self.cluster_context,
                    "config_file": config_file,
                    "in_cluster": self.in_cluster,
                }
                hook = KubernetesHookAsync(conn_id=None, **hook_params)

                async with await hook.get_api_client_async() as api_client:
                    v1_api = CoreV1Api(api_client)
                    state = await self.wait_for_pod_start(v1_api)
                    if state in PodPhase.terminal_states:
                        event = TriggerEvent({"status": "done"})
                    else:
                        event = await self.wait_for_container_completion(v1_api)
                yield event
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
