from unittest import mock
from unittest.mock import MagicMock

import pendulum
import pytest
from airflow.models import DAG, DagRun, TaskInstance
from airflow.utils import timezone

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperatorAsync,
    PodNotFoundException,
)
from astronomer.providers.cncf.kubernetes.triggers.wait_container import (
    PodLaunchTimeoutException,
)


def create_context(task):
    dag = DAG(dag_id="dag")
    tzinfo = pendulum.timezone("Europe/Amsterdam")
    execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(dag_id=dag.dag_id, execution_date=execution_date)
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "ts": execution_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


def test_raise_for_trigger_status_pending_timeout():
    with pytest.raises(PodLaunchTimeoutException):
        KubernetesPodOperatorAsync.raise_for_trigger_status(
            {
                "status": "error",
                "error_type": "PodLaunchTimeoutException",
                "description": "any message",
            }
        )


def test_raise_for_trigger_status_done():
    assert KubernetesPodOperatorAsync.raise_for_trigger_status({"status": "done"}) is None


@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.cleanup"
)
@mock.patch("airflow.kubernetes.kube_client.get_kube_client")
@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.raise_for_trigger_status"
)
@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.find_pod"
)
@mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
@mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.follow_container_logs")
def test_get_logs(
    follow_container_logs,
    await_pod_completion,
    find_pod,
    raise_for_trigger_status,
    get_kube_client,
    cleanup,
):
    pod = MagicMock()
    find_pod.return_value = pod
    op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
    context = create_context(op)
    await_pod_completion.return_value = None
    op.execute_complete(context, None)
    follow_container_logs.is_called_with(pod, "base")


@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.cleanup"
)
@mock.patch("airflow.kubernetes.kube_client.get_kube_client")
@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.raise_for_trigger_status"
)
@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.find_pod"
)
@mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
@mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.follow_container_logs")
def test_no_pod(
    follow_container_logs,
    await_pod_completion,
    find_pod,
    raise_for_trigger_status,
    get_kube_client,
    cleanup,
):
    find_pod.return_value = None
    op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
    context = create_context(op)
    with pytest.raises(PodNotFoundException):
        op.execute_complete(context, None)


@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.cleanup"
)
@mock.patch("airflow.kubernetes.kube_client.get_kube_client")
@mock.patch(
    "astronomer.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync.find_pod"
)
@mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
@mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.follow_container_logs")
def test_trigger_error(
    follow_container_logs,
    await_pod_completion,
    find_pod,
    get_kube_client,
    cleanup,
):
    find_pod.return_value = MagicMock()
    op = KubernetesPodOperatorAsync(task_id="test_task", name="test-pod", get_logs=True)
    with pytest.raises(PodLaunchTimeoutException):
        context = create_context(op)
        op.execute_complete(
            context,
            {
                "status": "error",
                "error_type": "PodLaunchTimeoutException",
                "description": "any message",
            },
        )
