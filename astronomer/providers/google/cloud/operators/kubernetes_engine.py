"""This module contains Google GKE operators."""
from __future__ import annotations

from typing import Any, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from kubernetes.client import models as k8s

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    PodNotFoundException,
)
from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    PodLaunchTimeoutException,
)
from astronomer.providers.google.cloud.gke_utils import _get_gke_config_file
from astronomer.providers.google.cloud.triggers.kubernetes_engine import (
    GKEStartPodTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class GKEStartPodOperatorAsync(KubernetesPodOperator):
    """
    Executes a task in a Kubernetes pod in the specified Google Kubernetes
    Engine cluster

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    The **minimum** required to define a cluster to create are the variables
    ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
    ``namespace``, and ``image``
    .. seealso::

        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

    :param location: The name of the Google Kubernetes Engine zone or region in which the
        cluster resides, e.g. 'us-central1-a'
    :param cluster_name: The name of the Google Kubernetes Engine cluster the pod
        should be spawned in
    :param use_internal_ip: Use the internal IP address as the endpoint
    :param project_id: The Google Developers Console project ID
    :param gcp_conn_id: The google cloud connection ID to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param regional: The location param is whether a region or a zone
    :param is_delete_operator_pod: What to do when the pod reaches its final
        state, or the execution is interrupted. If True, delete the
        pod; if False, leave the pod.  Current default is False, but this will be
        changed in the next major release of this provider.
    """

    def __init__(
        self,
        *,
        location: str,
        cluster_name: str,
        use_internal_ip: bool = False,
        project_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        regional: bool = False,
        poll_interval: float = 5,
        logging_interval: int | None = None,
        do_xcom_push: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.cluster_name = cluster_name
        self.gcp_conn_id = gcp_conn_id
        self.use_internal_ip = use_internal_ip
        self.impersonation_chain = impersonation_chain
        self.regional = regional
        self.poll_interval = poll_interval
        self.logging_interval = logging_interval
        self.do_xcom_push = do_xcom_push

        self.pod_name: str = ""
        self.pod_namespace: str = ""

    def _get_or_create_pod(self, context: Context) -> None:
        """A wrapper to fetch GKE config and get or create a pod"""
        with _get_gke_config_file(
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            impersonation_chain=self.impersonation_chain,
            regional=self.regional,
            location=self.location,
            use_internal_ip=self.use_internal_ip,
            cluster_context=self.cluster_context,
        ) as config_file:
            self.config_file = config_file
            self.pod_request_obj = self.build_pod_request_obj(context)
            self.pod: k8s.V1Pod = self.get_or_create_pod(self.pod_request_obj, context)
            self.pod_name = self.pod.metadata.name
            self.pod_namespace = self.pod.metadata.namespace

    def execute(self, context: Context) -> Any:
        """Look for a pod, if not found then create one and defer"""
        self._get_or_create_pod(context)
        self.log.info("Created pod=%s in namespace=%s", self.pod_name, self.pod_namespace)

        event = None
        try:
            with _get_gke_config_file(
                gcp_conn_id=self.gcp_conn_id,
                project_id=self.project_id,
                cluster_name=self.cluster_name,
                impersonation_chain=self.impersonation_chain,
                regional=self.regional,
                location=self.location,
                use_internal_ip=self.use_internal_ip,
                cluster_context=self.cluster_context,
            ) as config_file:
                hook_params: dict[str, Any] = {
                    "cluster_context": self.cluster_context,
                    "config_file": config_file,
                    "in_cluster": self.in_cluster,
                }
                hook = KubernetesHook(conn_id=None, **hook_params)
                client = hook.core_v1_client
                pod = client.read_namespaced_pod(self.pod_name, self.pod_namespace)
                phase = pod.status.phase
                if phase == PodPhase.SUCCEEDED:
                    event = {"status": "done", "namespace": self.namespace, "pod_name": self.name}

                elif phase == PodPhase.FAILED:
                    event = {
                        "status": "failed",
                        "namespace": self.namespace,
                        "pod_name": self.name,
                        "description": "Failed to start pod operator",
                    }
        except Exception as e:
            event = {"status": "error", "message": str(e)}

        if event:
            return self.trigger_reentry(context, event)

        self.defer(
            trigger=GKEStartPodTrigger(
                namespace=self.pod_namespace,
                name=self.pod_name,
                in_cluster=self.in_cluster,
                cluster_context=self.cluster_context,
                location=self.location,
                cluster_name=self.cluster_name,
                use_internal_ip=self.use_internal_ip,
                project_id=self.project_id,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                regional=self.regional,
                poll_interval=self.poll_interval,
                pending_phase_timeout=self.startup_timeout_seconds,
                logging_interval=self.logging_interval,
            ),
            method_name=self.trigger_reentry.__name__,
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> Any:  # type: ignore[override]
        """Callback for trigger once task reach terminal state"""
        self.trigger_reentry(context=context, event=event)

    @staticmethod
    def raise_for_trigger_status(event: dict[str, Any]) -> None:
        """Raise exception if pod is not in expected state."""
        if event["status"] == "error":
            description = event["description"]
            if "error_type" in event and event["error_type"] == "PodLaunchTimeoutException":
                raise PodLaunchTimeoutException(description)
            else:
                raise AirflowException(description)

    def trigger_reentry(self, context: Context, event: dict[str, Any]) -> Any:
        """
        Point of re-entry from trigger.

        If ``logging_interval`` is None, then at this point the pod should be done and we'll just fetch
        the logs and exit.

        If ``logging_interval`` is not None, it could be that the pod is still running and we'll just
        grab the latest logs and defer back to the trigger again.
        """
        remote_pod = None
        self.raise_for_trigger_status(event)
        try:
            with _get_gke_config_file(
                gcp_conn_id=self.gcp_conn_id,
                project_id=self.project_id,
                cluster_name=self.cluster_name,
                impersonation_chain=self.impersonation_chain,
                regional=self.regional,
                location=self.location,
                use_internal_ip=self.use_internal_ip,
            ) as config_file:
                self.config_file = config_file
                self.pod = self.find_pod(
                    namespace=event["namespace"],
                    context=context,
                )

                if not self.pod:
                    raise PodNotFoundException("Could not find pod after resuming from deferral")

                if self.get_logs:
                    last_log_time = event and event.get("last_log_time")
                    if last_log_time:
                        self.log.info("Resuming logs read from time %r", last_log_time)  # pragma: no cover
                    self.pod_manager.fetch_container_logs(
                        pod=self.pod,
                        container_name=self.BASE_CONTAINER_NAME,
                        follow=self.logging_interval is None,
                        since_time=last_log_time,
                    )

                if self.do_xcom_push:
                    result = self.extract_xcom(pod=self.pod)
                remote_pod = self.pod_manager.await_pod_completion(self.pod)
        except Exception:
            self.cleanup(
                pod=self.pod,
                remote_pod=remote_pod,
            )
            raise
        self.cleanup(
            pod=self.pod,
            remote_pod=remote_pod,
        )
        if self.do_xcom_push:
            ti = context["ti"]
            ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
            ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)
            return result
