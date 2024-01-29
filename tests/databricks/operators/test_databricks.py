from __future__ import annotations

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)

from astronomer.providers.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)

CONN_ID = "databricks_default"


class TestDatabricksSubmitRunOperatorAsync:
    def test_init(self):
        task = DatabricksSubmitRunOperatorAsync(
            task_id="submit_run",
            databricks_conn_id=CONN_ID,
            existing_cluster_id="xxxx-xxxxxx-xxxxxx",
            notebook_task={"notebook_path": "/Users/test@astronomer.io/Quickstart Notebook"},
        )

        assert isinstance(task, DatabricksSubmitRunOperator)
        assert task.deferrable is True


class TestDatabricksRunNowOperatorAsync:
    def test_init(self):
        task = DatabricksRunNowOperatorAsync(
            task_id="run_now",
            databricks_conn_id=CONN_ID,
        )

        assert isinstance(task, DatabricksRunNowOperator)
        assert task.deferrable is True
