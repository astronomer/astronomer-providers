import warnings
from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s
from pendulum import DateTime

from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    PodLaunchTimeoutException,
    WaitContainerTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class PodNotFoundException(AirflowException):
    """Expected pod does not exist in kube-api."""


class KubernetesPodOperatorAsync(KubernetesPodOperator):
    """
    Async (deferring) version of KubernetesPodOperator

    .. warning::
        By default, logs will not be available in the Airflow Webserver until the task completes. However,
        you can configure ``KubernetesPodOperatorAsync`` to periodically resume and fetch logs.  This behavior
        is controlled by param ``logging_interval``.

    :param poll_interval: interval in seconds to sleep between checking pod status
    :param logging_interval: max time in seconds that task should be in deferred state before
        resuming to fetch latest logs. If ``None``, then the task will remain in deferred state until pod
        is done, and no logs will be visible until that time.
    """

    def __init__(self, *, poll_interval: int = 5, logging_interval: Optional[int] = None, **kwargs: Any):
        self.poll_interval = poll_interval
        self.logging_interval = logging_interval
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

    def defer(self, last_log_time: Optional[DateTime] = None, **kwargs: Any) -> None:
        """Defers to ``WaitContainerTrigger`` optionally with last log time."""
        if kwargs:
            raise ValueError(
                f"Received keyword arguments {list(kwargs.keys())} but "
                f"they are not used in this implementation of `defer`."
            )
        super().defer(
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
                logging_interval=self.logging_interval,
                last_log_time=last_log_time,
            ),
            method_name=self.trigger_reentry.__name__,
        )

    def execute(self, context: Context) -> None:  # noqa: D102
        self.pod_request_obj = self.build_pod_request_obj(context)
        self.pod: k8s.V1Pod = self.get_or_create_pod(self.pod_request_obj, context)
        self.defer()

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> Any:
        """Deprecated; replaced by trigger_reentry."""
        warnings.warn(
            "Method `execute_complete` is deprecated and replaced with method `trigger_reentry`.",
            DeprecationWarning,
        )
        self.trigger_reentry(context=context, event=event)

    def trigger_reentry(self, context: Context, event: Dict[str, Any]) -> Any:
        """
        Point of re-entry from trigger.

        If ``logging_interval`` is None, then at this point the pod should be done and we'll just fetch
        the logs and exit.

        If ``logging_interval`` is not None, it could be that the pod is still running and we'll just
        grab the latest logs and defer back to the trigger again.
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
                last_log_time = event and event.get("last_log_time")
                if last_log_time:
                    self.log.info("Resuming logs read from time %r", last_log_time)
                pod_log_status = self.pod_manager.fetch_container_logs(
                    pod=self.pod,
                    container_name=self.BASE_CONTAINER_NAME,
                    follow=self.logging_interval is None,
                    since_time=last_log_time,
                )
                if pod_log_status.running:
                    self.log.info("Container still running; deferring again.")
                    self.defer(pod_log_status.last_log_time)

            if self.do_xcom_push:
                result = self.extract_xcom(pod=self.pod)
            remote_pod = self.pod_manager.await_pod_completion(self.pod)
        except TaskDeferred:
            raise
        except Exception:
            self.cleanup(
                pod=self.pod or self.pod_request_obj,
                remote_pod=remote_pod,
            )
            raise
        self.cleanup(
            pod=self.pod or self.pod_request_obj,
            remote_pod=remote_pod,
        )
        ti = context["ti"]
        ti.xcom_push(key="pod_name", value=self.pod.metadata.name)
        ti.xcom_push(key="pod_namespace", value=self.pod.metadata.namespace)
        if self.do_xcom_push:
            return result
