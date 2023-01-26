from unittest import mock

from astronomer.providers.databricks.operators.notebook import (
    DatabricksNotebookOperator,
)
from astronomer.providers.databricks.task_group.workflow import (
    DatabricksWorkflowTaskGroup,
)

expected_workflow_json = {
    "email_notifications": {"no_alert_for_skipped_runs": False},
    "format": "MULTI_TASK",
    "job_clusters": [{"job_cluster_key": "foo"}],
    "max_concurrent_runs": 1,
    "name": "unit_test_dag.test_workflow",
    "tasks": [
        {
            "depends_on": [],
            "email_notifications": {},
            "job_cluster_key": "foo",
            "notebook_task": {"base_parameters": {}, "notebook_path": "/foo/bar", "source": "WORKSPACE"},
            "task_key": "unit_test_dag__test_workflow__notebook_1",
            "timeout_seconds": 0,
        },
        {
            "depends_on": [{"task_key": "unit_test_dag__test_workflow__notebook_1"}],
            "email_notifications": {},
            "job_cluster_key": "foo",
            "notebook_task": {
                "base_parameters": {"foo": "bar"},
                "notebook_path": "/foo/bar",
                "source": "WORKSPACE",
            },
            "task_key": "unit_test_dag__test_workflow__notebook_2",
            "timeout_seconds": 0,
        },
    ],
    "timeout_seconds": 0,
}


@mock.patch("astronomer.providers.databricks.task_group.workflow.DatabricksHook")
@mock.patch("astronomer.providers.databricks.task_group.workflow.ApiClient")
@mock.patch("astronomer.providers.databricks.task_group.workflow.JobsApi")
def test_create_workflow_from_notebooks_with_create(mock_jobs_api, mock_api, mock_hook, dag):
    mock_jobs_api.return_value.create_job.return_value = {"job_id": 1}
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
                },
            )
            notebook_1 >> notebook_2

    assert len(task_group.children) == 3
    assert task_group.children["test_workflow.launch"].create_workflow_json() == expected_workflow_json
    task_group.children["test_workflow.launch"].execute(context={})
    mock_jobs_api.return_value.create_job.assert_called_once_with(
        json=expected_workflow_json,
    )
    mock_jobs_api.return_value.run_now.assert_called_once_with(
        job_id=1,
        jar_params=[],
        notebook_params=[{"notebook_path": "/foo/bar"}],
        python_params=[],
        spark_submit_params=[],
    )


@mock.patch("astronomer.providers.databricks.task_group.workflow.DatabricksHook")
@mock.patch("astronomer.providers.databricks.task_group.workflow.ApiClient")
@mock.patch("astronomer.providers.databricks.task_group.workflow.JobsApi")
@mock.patch("astronomer.providers.databricks.task_group.workflow._get_job_by_name")
def test_create_workflow_from_notebooks_existing_job(mock_get_jobs, mock_jobs_api, mock_api, mock_hook, dag):
    mock_get_jobs.return_value = {"job_id": 1}
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
                },
            )
            notebook_1 >> notebook_2

    assert len(task_group.children) == 3
    assert task_group.children["test_workflow.launch"].create_workflow_json() == expected_workflow_json
    task_group.children["test_workflow.launch"].execute(context={})
    mock_jobs_api.return_value.reset_job.assert_called_once_with(
        json={"job_id": 1, "new_settings": expected_workflow_json},
    )
    mock_jobs_api.return_value.run_now.assert_called_once_with(
        job_id=1,
        jar_params=[],
        notebook_params=[{"notebook_path": "/foo/bar"}],
        python_params=[],
        spark_submit_params=[],
    )


# TODO should we delete this test? It doesn't seem like there are integration tests in
# this library

# actual_databricks_job = {
#     "job_id": 183826003004249,
#     "creator_user_name": "daniel@astronomer.io",
#     "run_as_user_name": "daniel@astronomer.io",
#     "run_as_owner": True,
#     "settings": {
#         "name": "Testing databricks workflows",
#         "email_notifications": {
#             "no_alert_for_skipped_runs": False
#         },
#         "webhook_notifications": {},
#         "timeout_seconds": 0,
#         "max_concurrent_runs": 1,
#         "tasks": [
#             {
#                 "task_key": "first_notebook",
#                 "notebook_task": {
#                     "notebook_path": "/Users/daniel@astronomer.io/Test workflow",
#                     "base_parameters": {
#                         "foo": "bar"
#                     },
#                     "source": "WORKSPACE"
#                 },
#                 "job_cluster_key": "Shared_job_cluster",
#                 "timeout_seconds": 0,
#                 "email_notifications": {}
#             },
#             {
#                 "task_key": "second_notebook",
#                 "depends_on": [
#                     {
#                         "task_key": "first_notebook"
#                     }
#                 ],
#                 "notebook_task": {
#                     "notebook_path": "/Users/daniel@astronomer.io/Test workflow",
#                     "base_parameters": {
#                         "biz": "baz"
#                     },
#                     "source": "WORKSPACE"
#                 },
#                 "job_cluster_key": "Shared_job_cluster",
#                 "timeout_seconds": 0,
#                 "email_notifications": {}
#             }
#         ],
#         "job_clusters": [
#             {
#                 "job_cluster_key": "Shared_job_cluster",
#                 "new_cluster": {
#                     "cluster_name": "",
#                     "spark_version": "11.3.x-scala2.12",
#                     "aws_attributes": {
#                         "first_on_demand": 1,
#                         "availability": "SPOT_WITH_FALLBACK",
#                         "zone_id": "us-east-2b",
#                         "spot_bid_price_percent": 100,
#                         "ebs_volume_count": 0
#                     },
#                     "node_type_id": "i3.xlarge",
#                     "spark_env_vars": {
#                         "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
#                     },
#                     "enable_elastic_disk": False,
#                     "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
#                     "runtime_engine": "STANDARD",
#                     "num_workers": 8
#                 }
#             }
#         ],
#         "format": "MULTI_TASK"
#     },
#     "created_time": 1674670074168
# }
#
#
# def test_run_db_dag(dag):
#     job_cluster_spec = [
#         {
#             "job_cluster_key": "Shared_job_cluster",
#             "new_cluster": {
#                 "cluster_name": "",
#                 "spark_version": "11.3.x-scala2.12",
#                 "aws_attributes": {
#                     "first_on_demand": 1,
#                     "availability": "SPOT_WITH_FALLBACK",
#                     "zone_id": "us-east-2b",
#                     "spot_bid_price_percent": 100,
#                     "ebs_volume_count": 0
#                 },
#                 "node_type_id": "i3.xlarge",
#                 "spark_env_vars": {
#                     "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
#                 },
#                 "enable_elastic_disk": False,
#                 "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
#                 "runtime_engine": "STANDARD",
#                 "num_workers": 8
#             }
#         }
#     ]
#     with dag:
#         task_group = DatabricksWorkflowTaskGroup(
#             group_id="test_workflow",
#             databricks_conn_id="databricks_conn",
#             job_clusters=job_cluster_spec,
#             notebook_params=[],
#         )
#         with task_group:
#             notebook_1 = DatabricksNotebookOperator(
#                 task_id="notebook_1",
#                 databricks_conn_id="databricks_conn",
#                 notebook_path="/Users/daniel@astronomer.io/Test workflow",
#                 source="WORKSPACE",
#                 job_cluster_key="Shared_job_cluster",
#             )
#             notebook_2 = DatabricksNotebookOperator(
#                 task_id="notebook_2",
#                 databricks_conn_id="databricks_conn",
#                 notebook_path="/Users/daniel@astronomer.io/Test workflow",
#                 source="WORKSPACE",
#                 job_cluster_key="Shared_job_cluster",
#                 notebook_params={
#                     "foo": "bar",
#                 },
#             )
#             notebook_1 >> notebook_2
#         dag.test(conn_file_path="")
