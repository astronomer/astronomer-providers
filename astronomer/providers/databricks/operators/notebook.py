"""DatabricksNotebookOperator for submitting notebook jobs to databricks."""
from __future__ import annotations

import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.operator import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient

from astronomer.providers.utils.typing_compat import Context


class DatabricksNotebookOperator(BaseOperator):
    """
    Launches a notebook to databricks using an Airflow operator.

    The DatabricksNotebookOperator allows users to launch and monitor notebook deployments on Databricks as Aiflow tasks.
    It can be used as a part of a DatabricksWorkflowTaskGroup to take advantage of job clusters,
    which allows users to run their tasks on cheaper clusters that can be shared between tasks.

    Here is an example of running a notebook as a part of a workflow task group:

    .. code-block: python

        with dag:
            task_group = DatabricksWorkflowTaskGroup(
                group_id="test_workflow",
                databricks_conn_id="databricks_conn",
                job_clusters=job_cluster_spec,
                notebook_params=[],
            )
            with task_group:
                notebook_1 = DatabricksNotebookOperator(
                    task_id="notebook_1",
                    databricks_conn_id="databricks_conn",
                    notebook_path="/Users/daniel@astronomer.io/Test workflow",
                    source="WORKSPACE",
                    job_cluster_key="Shared_job_cluster",
                )
                notebook_2 = DatabricksNotebookOperator(
                    task_id="notebook_2",
                    databricks_conn_id="databricks_conn",
                    notebook_path="/Users/daniel@astronomer.io/Test workflow",
                    source="WORKSPACE",
                    job_cluster_key="Shared_job_cluster",
                    notebook_params={
                        "foo": "bar",
                    },
                )
                notebook_1 >> notebook_2


        :param notebook_path: the path to the notebook in Databricks
        :param source: Optional location type of the notebook. When set to WORKSPACE, the notebook will be retrieved
        from the local Databricks workspace. When set to GIT, the notebook will be retrieved from a Git repository
        defined in git_source. If the value is empty, the task will use GIT if git_source is defined
        and WORKSPACE otherwise. For more information please visit
        https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
        :param databricks_conn_id: the connection id to use to connect to Databricks
        :param notebook_params: the parameters to pass to the notebook
        :param kwargs:
    """

    template_fields = "databricks_run_id"

    def __init__(
        self,
        notebook_path: str,
        source: str,
        databricks_conn_id: str,
        notebook_params: dict | None = None,
        **kwargs,
    ):
        """
        Create a Databricks notebook operator.

        :param notebook_path: the path to the notebook in Databricks
        :param source: Optional location type of the notebook. When set to WORKSPACE, the notebook will be retrieved
        from the local Databricks workspace. When set to GIT, the notebook will be retrieved from a Git repository
        defined in git_source. If the value is empty, the task will use GIT if git_source is defined
        and WORKSPACE otherwise. For more information please visit
        https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate
        :param databricks_conn_id: the connection id to use to connect to Databricks
        :param notebook_params: the parameters to pass to the notebook
        :param kwargs:
        """
        self.notebook_path = notebook_path
        self.source = source
        self.notebook_params = notebook_params or {}
        self.databricks_conn_id = databricks_conn_id
        self.databricks_run_id = ""
        self.job_cluster_key = kwargs.pop("job_cluster_key", "")
        super().__init__(**kwargs)

    def convert_to_databricks_workflow_task(self, relevant_upstreams):
        """
        Convert the operator to a Databricks workflow task that can be task in a workflow.

        :type relevant_upstreams: list[BaseOperator]
        :type job_cluster_key: dict
        """
        result = {
            "task_key": self.dag_id + "__" + self.task_id.replace(".", "__"),
            "depends_on": [
                {"task_key": self.dag_id + "__" + t.replace(".", "__")}
                for t in self.upstream_task_ids
                if t in relevant_upstreams
            ],
            "job_cluster_key": self.job_cluster_key,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notebook_task": {
                "notebook_path": self.notebook_path,
                "source": self.source,
                "base_parameters": self.notebook_params,
            },
        }
        return result

    def monitor_databricks_job(self):
        """Monitor the Databricks job until it completes. Raises Airflow exception if the job fails."""
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        api_client = ApiClient(
            host=databricks_conn.host,
            token=databricks_conn.password,
        )
        runs_api = RunsApi(api_client)
        current_task = {x["task_key"]: x for x in runs_api.get_run(self.databricks_run_id)["tasks"]}[
            self.dag_id + "__" + self.task_id.replace(".", "__")
        ]
        while runs_api.get_run(current_task["run_id"])["state"]["life_cycle_state"] == "PENDING":
            print(f"task {self.task_id.replace('.', '__')} pending")
            time.sleep(5)

        while runs_api.get_run(current_task["run_id"])["state"]["life_cycle_state"] == "RUNNING":
            print(f"task {self.task_id.replace('.', '__')} running")
            time.sleep(5)

        final_state = runs_api.get_run(current_task["run_id"])["state"]
        if final_state.get("life_cycle_state", None) != "TERMINATED":
            raise AirflowException(
                f"Databricks job failed with state {final_state}. Message: {final_state['state_message']}"
            )
        if final_state["result_state"] != "SUCCESS":
            raise AirflowException("Task failed. Reason: %s", final_state["state_message"])

    def launch_notebook_job(self):
        """Launch the notebook as a one-time job to Databricks."""
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        api_client = ApiClient(
            user=databricks_conn.login, password=databricks_conn.password, host=databricks_conn.host
        )
        runs_api = RunsApi(api_client)
        run = runs_api.submit_run(
            {
                "new_cluster": {"spark_version": "6.6.x-scala2.11", "node_type_id": "Standard_DS3_v2"},
                "notebook_task": {
                    "notebook_path": self.notebook_path,
                    "base_parameters": {"source": self.source},
                },
            }
        )
        self.databricks_run_id = run["run_id"]
        return run

    def execute(self, context: Context) -> Any:
        """
        Execute the DataBricksNotebookOperator.

        Executes the DataBricksNotebookOperator. If the task is inside of a
        DatabricksWorkflowTaskGroup, it assumes the notebook is already launched
        and proceeds to monitor the running notebook.

        :param context:
        :return:
        """
        if not (hasattr(self.task_group, "is_databricks") and getattr(self.task_group, "is_databricks")):
            self.launch_notebook_job()
        self.monitor_databricks_job()
