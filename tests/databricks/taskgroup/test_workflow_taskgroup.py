from unittest import mock

from astronomer.providers.databricks.operators.notebook import (
    DatabricksNotebookOperator,
)
from astronomer.providers.databricks.task_group.workflow_taskgroup import (
    DatabricksWorkflowTaskGroup,
)

expected_workflow_json = {'email_notifications': {'no_alert_for_skipped_runs': False},
 'format': 'MULTI_TASK',
 'max_concurrent_runs': 1,
 'name': 'unit_test_dag_test_workflow_test_workflow.launch',
 'tasks': [{'depends_on': [],
            'email_notifications': {},
            'job_cluster_key': 'foo',
            'notebook_task': {'base_parameters': {},
                              'notebook_path': '/foo/bar',
                              'source': 'WORKSPACE'},
            'task_key': 'test_workflow__notebook_1',
            'timeout_seconds': 0},
           {'depends_on': [{'task_key': 'test_workflow__notebook_1'}],
            'email_notifications': {},
            'job_cluster_key': 'foo',
            'notebook_task': {'base_parameters': {'foo': 'bar'},
                              'notebook_path': '/foo/bar',
                              'source': 'WORKSPACE'},
            'task_key': 'test_workflow__notebook_2',
            'timeout_seconds': 0}],
 'timeout_seconds': 0}

@mock.patch("astronomer.providers.databricks.task_group.workflow_taskgroup.DatabricksHook")
@mock.patch("astronomer.providers.databricks.task_group.workflow_taskgroup.ApiClient")
@mock.patch("astronomer.providers.databricks.task_group.workflow_taskgroup.JobsApi")
def test_create_workflow_from_notebooks(mock_jobs_api, mock_api, mock_hook, dag):
    with dag:
        task_group = DatabricksWorkflowTaskGroup(
            group_id="test_workflow",
            databricks_conn_id="foo",
            job_clusters=[{"job_cluster_key": "foo"}],
            notebook_params=[{"notebook_path": "/foo/bar"}],
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
                notebook_params={
                    "foo": "bar",
                }
            )
            notebook_1 >> notebook_2

    assert len(task_group.children) == 3
    assert task_group.children["test_workflow.launch"].create_workflow_json() == expected_workflow_json
    task_group.children["test_workflow.launch"].execute(context={})
    mock_jobs_api.return_value.create_job.assert_called_once_with(
        json=expected_workflow_json,
    )
    mock_jobs_api.return_value.run_now.assert_called_once_with(
        job_id=None,
        jar_params=[],
        notebook_params=[{"notebook_path": "/foo/bar"}],
        python_params=[],
        spark_submit_params=[],
    )
