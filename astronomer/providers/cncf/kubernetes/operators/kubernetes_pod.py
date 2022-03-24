from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from airflow.utils.context import Context

from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    PodLaunchTimeoutException,
    WaitContainerTrigger,
)


class PodNotFoundException(AirflowException):
    """Expected pod does not exist in kube-api."""


class KubernetesPodOperatorAsync(KubernetesPodOperator):
    """
    Async (deferring) version of KubernetesPodOperator

    .. warning::
        The logs would not be available in the Airflow Webserver until the task completes. This is
        the main difference between this operator and the
        :class:`~airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator`.

    :param poll_interval: interval in seconds to sleep between checking pod status
    """

    def __init__(self, *, poll_interval: int = 5, **kwargs: Any):
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    @staticmethod
    def raise_for_trigger_status(event: Dict[str, Any]) -> None:
        """Raise exception if pod is not in expected state."""
        if event["status"] == "error":
            error_type = event["error_type"]
            description = event["description"]
            if error_type == "PodLaunchTimeoutException":
                raise PodLaunchTimeoutException(description)
            else:
                raise AirflowException(description)

    def execute(self, context: Context) -> None:  # noqa: D102
        self.pod_request_obj = self.build_pod_request_obj(context)
        self.pod = self.get_or_create_pod(self.pod_request_obj, context)
        self.defer(
            trigger=WaitContainerTrigger(
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
            ),
            method_name=self.execute_complete.__name__,
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        remote_pod = None
        try:
            self.pod_request_obj = self.build_pod_request_obj(context)
            self.pod = self.find_pod(
                namespace=self.namespace or self.pod_request_obj.metadata.namespace,
                context=context,
            )

            # we try to find pod before possibly raising so that on_kill will have `pod` attr
            self.raise_for_trigger_status(event)

            if not self.pod:
                raise PodNotFoundException("Could not find pod after resuming from deferral")

            if self.get_logs:
                self.pod_manager.follow_container_logs(
                    pod=self.pod,
                    container_name=self.BASE_CONTAINER_NAME,
                )
            if self.do_xcom_push:
                result = self.extract_xcom(pod=self.pod)
            remote_pod = self.pod_manager.await_pod_completion(self.pod)
        finally:
            self.cleanup(
                pod=self.pod or self.pod_request_obj,
                remote_pod=remote_pod,
            )
        ti = context["ti"]
        ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
        ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)
        if self.do_xcom_push:
            return result
