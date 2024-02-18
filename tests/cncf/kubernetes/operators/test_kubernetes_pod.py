from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

from astronomer.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperatorAsync,
)


class TestKubernetesPodOperatorAsync:
    def test_init(self):
        task = KubernetesPodOperatorAsync(
            task_id="test_task", name="test-pod", get_logs=True, logging_interval=5
        )

        assert isinstance(task, KubernetesPodOperator)
        assert task.deferrable is True
