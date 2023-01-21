from unittest import mock

from astronomer.providers.databricks.operators.notebook import (
    DatabricksNotebookOperator,
)
from astronomer.providers.databricks.task_group.workflow_taskgroup import (
    DatabricksWorkflowTaskGroup,
)

expected_workflow_json = {
    "email_notifications": {"no_alert_for_skipped_runs": False},
    "format": "MULTI_TASK",
    "max_concurrent_runs": 1,
    "name": "airflow-job-test",
    "tasks": [
        {
            "depends_on": [],
            "email_notifications": {},
            "existing_cluster_id": "bar",
            "notebook_task": {"base_parameters": {}, "notebook_path": "/foo/bar", "source": "WORKSPACE"},
            "task_key": "test_workflow__notebook_1",
            "timeout_seconds": 0,
        },
        {
            "depends_on": [{"task_key": "test_workflow__notebook_1"}],
            "email_notifications": {},
            "existing_cluster_id": "bar",
            "notebook_task": {"base_parameters": {}, "notebook_path": "/foo/bar", "source": "WORKSPACE"},
            "task_key": "test_workflow__notebook_2",
            "timeout_seconds": 0,
        },
    ],
    "timeout_seconds": 0,
}


@mock.patch("astronomer.providers.databricks.task_group.workflow_taskgroup.DatabricksHook")
@mock.patch("astronomer.providers.databricks.task_group.workflow_taskgroup.ApiClient")
@mock.patch("astronomer.providers.databricks.task_group.workflow_taskgroup.ClusterApi")
@mock.patch("astronomer.providers.databricks.task_group.workflow_taskgroup.RunsApi")
def test_create_workflow_from_notebooks(mock_runs_api, mock_cluster, mock_api, mock_hook, dag):
    mock_cluster.return_value.create_cluster.return_value = {"cluster_id": "bar"}
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_clusters=[{"job_cluster_key": "foo"}],
        )
        with task_group:
            notebook_1 = DatabricksNotebookOperator(
                task_id="notebook_1",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
            )
            notebook_2 = DatabricksNotebookOperator(
                task_id="notebook_2",
                databricks_conn_id="foo",
                notebook_path="/foo/bar",
                source="WORKSPACE",
                job_cluster_key="foo",
            )
            notebook_1 >> notebook_2

    assert len(task_group.children) == 3
    assert (
        task_group.children["test_workflow.launch"].create_workflow_json({"foo": "bar"})
        == expected_workflow_json
    )
    task_group.children["test_workflow.launch"].execute(context={})
    assert mock_runs_api.mock_calls[1].kwargs["json"] == expected_workflow_json
