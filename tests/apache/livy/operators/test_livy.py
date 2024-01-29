from __future__ import annotations

from airflow.providers.apache.livy.operators.livy import LivyOperator

from astronomer.providers.apache.livy.operators.livy import LivyOperatorAsync


class TestLivyOperatorAsync:
    def test_init(self):
        task = LivyOperatorAsync(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            task_id="livy_example",
        )

        assert isinstance(task, LivyOperator)
        assert task.deferrable is True
