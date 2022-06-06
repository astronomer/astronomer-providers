"""This module contains Google GKE operators."""
from typing import Any, Dict, Optional, Sequence, Union

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.utils.context import Context
from kubernetes.client import models as k8s

from astronomer.providers.google.cloud.triggers.kubernetes_engine import (
    GKEStartPodTrigger,
)


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
    :param use_internal_ip: Use the internal IP address as the endpoint.
    :param project_id: The Google Developers Console project id
    :param gcp_conn_id: The google cloud connection id to use. This allows for
        users to specify a service account.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param regional: The location param is region name.
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
        project_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        regional: bool = False,
        poll_interval: float = 5,
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
        self.pod_name = None
        self.pod_namespace = None
        self.poll_interval = poll_interval

    def get_gke_config_file(self, context: "Context") -> None:
        """A wrapper over `GKEStartPodOperator.get_gke_config_file` to fetch GKE config"""
        with GKEStartPodOperator.get_gke_config_file(
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            impersonation_chain=self.impersonation_chain,
            regional=self.regional,
            location=self.location,
            use_internal_ip=self.use_internal_ip,
        ) as config_file:
            self.config_file = config_file
            self.pod_request_obj = self.build_pod_request_obj(context)
            self.pod: k8s.V1Pod = self.get_or_create_pod(self.pod_request_obj, context)
            self.pod_name = self.pod.metadata.name
            self.pod_namespace = self.pod.metadata.namespace

    def execute(self, context: "Context") -> None:
        """Look for a pod, if not found then create one and defer"""
        self.get_gke_config_file(context)
        self.log.info("Created pod=%s in namespace=%s", self.pod_name, self.pod_namespace)
        self.defer(
            trigger=GKEStartPodTrigger(
                namespace=self.pod.metadata.namespace,
                name=self.pod_name,
                in_cluster=self.in_cluster,
                cluster_context=self.cluster_context,
                is_delete_operator_pod=self.is_delete_operator_pod,
                location=self.location,
                cluster_name=self.cluster_name,
                use_internal_ip=self.use_internal_ip,
                project_id=self.project_id,
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                regional=self.regional,
                poll_interval=self.poll_interval,
                pending_phase_timeout=self.startup_timeout_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> Any:
        """Callback for trigger once task reach terminal state"""
        self.log.info("event = %s", event)
        self.log.info("context = %s", context)
