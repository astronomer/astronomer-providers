"""This module contains Google GKE operators."""
import tempfile
from typing import Any, Dict, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKEStartPodOperator,
)
from airflow.utils.context import Context
from pendulum import DateTime

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperatorAsync,
)
from astronomer.providers.google.cloud.triggers.wait_container import (
    GKEWaitContainerTrigger,
)


class GKEStartPodOperatorAsync(KubernetesPodOperatorAsync):
    """
    Executes a task asynchronously in a Kubernetes pod in the specified Google Kubernetes
    Engine cluster

    This Operator assumes that the system has gcloud installed and has configured a
    connection id with a service account.

    The **minimum** required to define a cluster to create are the variables
    ``task_id``, ``project_id``, ``location``, ``cluster_name``, ``name``,
    ``namespace``, and ``image``

        For more detail about Kubernetes Engine authentication have a look at the reference:
        https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl#internal_ip

        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GKEStartPodOperator`

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
    :param gke_yaml_config: The GKE yaml config.
    """

    template_fields: Sequence[str] = tuple(
        {"project_id", "location", "cluster_name"} | set(KubernetesPodOperatorAsync.template_fields)
    )

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
        gke_yaml_config: Optional[str] = None,
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

        # There is no need to manage the kube_config file, as it will be generated automatically.
        # All Kubernetes parameters (except config_file) are also valid for the GKEStartPodOperatorAsync.
        if self.config_file:
            raise AirflowException(
                "config_file is not an allowed parameter for the GKEStartPodOperatorAsync."
            )

        self.gke_yaml_config = gke_yaml_config

    def execute(self, context: "Context") -> None:
        """
        Airflow runs this method on the worker and defers using the trigger.
        Submit the job and get the job_id using which we defer and poll in trigger
        """
        with GKEStartPodOperator.get_gke_config_file(
            gcp_conn_id=self.gcp_conn_id,
            project_id=self.project_id,
            cluster_name=self.cluster_name,
            impersonation_chain=self.impersonation_chain,
            regional=self.regional,
            location=self.location,
            use_internal_ip=self.use_internal_ip,
        ) as config_file:
            with open(config_file) as tmp_file:
                self.gke_yaml_config = tmp_file.read()

            self.config_file = config_file
            return super().execute(context)

    def trigger_reentry(self, context: Context, event: Dict[str, Any]) -> Any:
        """Point of re-entry from trigger."""
        with tempfile.NamedTemporaryFile(mode="w+t") as conf_file:
            conf_file.write(self.gke_yaml_config)
            conf_file.seek(0)
            self.config_file = conf_file.name
            return super().trigger_reentry(context, event)

    def defer(self, last_log_time: Optional[DateTime] = None, **kwargs: Any) -> None:
        """Defers to ``GKEWaitContainerTrigger`` optionally with last log time."""
        if kwargs:
            raise ValueError(
                f"Received keyword arguments {list(kwargs.keys())} but "
                f"they are not used in this implementation of `defer`."
            )
        BaseOperator.defer(
            self,
            trigger=GKEWaitContainerTrigger(
                kubernetes_conn_id=None,
                hook_params={
                    "cluster_context": self.cluster_context,
                    "config_file": self.config_file,
                    "in_cluster": self.in_cluster,
                },
                pod_name=self.pod.metadata.name,
                container_name=self.BASE_CONTAINER_NAME,
                pod_namespace=self.pod.metadata.namespace,
                pending_phase_timeout=self.startup_timeout_seconds,
                poll_interval=self.poll_interval,
                logging_interval=self.logging_interval,
                last_log_time=last_log_time,
                gke_yaml_config=self.gke_yaml_config,
            ),
            method_name=self.trigger_reentry.__name__,
        )
