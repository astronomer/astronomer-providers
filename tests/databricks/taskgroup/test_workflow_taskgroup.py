from astronomer.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
)
from astronomer.providers.databricks.task_group.workflow_taskgroup import (
    DatabricksWorkflowTaskGroup,
)

expected_workflow_json = {
    "email_notifications": {"no_alert_for_skipped_runs": False},
    "format": "MULTI_TASK",
    "job_clusters": [{"job_cluster_key": "job_cluster_value"}],
    "max_concurrent_runs": 1,
    "name": "airflow-job-test",
    "tasks": [
        {
            "depends_on": [],
            "email_notifications": {},
            "job_cluster_key": "job_cluster_value",
            "notebook_task": {"notebook_path": "/foo/bar", "source": "WORKSPACE"},
            "task_key": "test_workflow__notebook_1",
            "timeout_seconds": 0,
        },
        {
            "depends_on": [{"task_key": "test_workflow__notebook_1"}],
            "email_notifications": {},
            "job_cluster_key": "job_cluster_value",
            "notebook_task": {"notebook_path": "/foo/bar", "source": "WORKSPACE"},
            "task_key": "test_workflow__notebook_2",
            "timeout_seconds": 0,
        },
    ],
    "timeout_seconds": 0,
}


def test_create_workflow_from_notebooks(dag):
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_cluster_json={"job_cluster_key": "job_cluster_value"},
        )
        with task_group:
            notebook_1 = DatabricksNotebookOperator(
                task_id="notebook_1", databricks_conn_id="foo", notebook_path="/foo/bar", source="WORKSPACE"
            )
            notebook_2 = DatabricksNotebookOperator(
                task_id="notebook_2", databricks_conn_id="foo", notebook_path="/foo/bar", source="WORKSPACE"
            )
            notebook_1 >> notebook_2

    assert len(task_group.children) == 3
    assert task_group.children["test_workflow.launch"].create_workflow_json() == expected_workflow_json
