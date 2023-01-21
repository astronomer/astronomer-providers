gfrom __future__ import annotations

from logging import Logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass
import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.operator import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.utils.task_group import TaskGroup
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient

from astronomer.providers.utils.typing_compat import Context


class CreateDatabricksWorkflowOperator(BaseOperator):
    def __init__(
        self,
        task_id,
        databricks_conn_id,
        job_clusters: list[dict[str, object]] = None,
        existing_clusters: list[str] = None,
        tasks_to_convert: list[BaseOperator] = None,
        **kwargs,
    ):
        self.existing_clusters = existing_clusters or []
        self.job_clusters = job_clusters or []
        self.job_cluster_dict = {j["job_cluster_key"]: j for j in self.job_clusters}
        self.tasks_to_convert = tasks_to_convert or []
        self.relevant_upstreams = [task_id]
        self.databricks_conn_id = databricks_conn_id
        self.databricks_run_id = None
        super().__init__(task_id=task_id, **kwargs)

    def add_task(self, task: BaseOperator):
        """
        Adds a task to the list of tasks to convert to a workflow
        :param task:
        :return:
        """
        self.tasks_to_convert.append(task)

    def create_workflow_json(self, job_cluster_dict) -> dict[str, object]:
        """
        Creates a workflow json that can be submitted to databricks
        :return:
        """
        task_json = [
            task.convert_to_databricks_workflow_task(
                relevant_upstreams=self.relevant_upstreams, job_cluster_key_map=job_cluster_dict
            )
            for task in self.tasks_to_convert
        ]
        full_json = {
            "name": "airflow-job-test",
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": task_json,
            "format": "MULTI_TASK",
        }
        return full_json

    def execute(self, context: Context) -> Any:
        job_existing_cluster_map = {}
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        api_client = ApiClient(
            user=databricks_conn.login, password=databricks_conn.password, host=databricks_conn.host
        )
        cluster_api = ClusterApi(api_client)
        for job_name, job_json in self.job_cluster_dict.items():
            cluster_id = cluster_api.create_cluster(job_json)
            job_existing_cluster_map[job_name] = cluster_id["cluster_id"]
        self.job_cluster_dict = job_existing_cluster_map

        runs_api = RunsApi(api_client)
        run_id = runs_api.submit_run(json=self.create_workflow_json(self.job_cluster_dict))

        while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "PENDING":
            print("job pending")
            time.sleep(5)
        return run_id


class CleanupDatabricksWorkflowOperator(BaseOperator):
    def __init__(
        self,
        task_id,
        databricks_conn_id,
        job_cluster_ids: list[str] = None,
        **kwargs,
    ):
        self.job_cluster_ids = job_cluster_ids or []
        self.databricks_conn_id = databricks_conn_id
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Context) -> Any:
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        api_client = ApiClient(
            user=databricks_conn.login, password=databricks_conn.password, host=databricks_conn.host
        )
        cluster_api = ClusterApi(api_client)
        for cluster_id in self.job_cluster_ids:
            cluster_api.delete_cluster(cluster_id)


class DatabricksWorkflowTaskGroup(TaskGroup):
    @property
    def log(self) -> Logger:
        pass

    is_databricks = True

    def __init__(self, databricks_conn_id, existing_clusters=None, job_clusters=None, **kwargs):
        self.databricks_conn_id = databricks_conn_id
        self.existing_clusters = existing_clusters or []
        self.job_clusters = job_clusters or []
        super().__init__(**kwargs)

    def __exit__(self, _type, _value, _tb):
        roots = self.roots
        create_databricks_workflow_task: CreateDatabricksWorkflowOperator = CreateDatabricksWorkflowOperator(
            dag=self.dag,
            task_id="launch",
            databricks_conn_id=self.databricks_conn_id,
            job_clusters=self.job_clusters,
            existing_clusters=self.existing_clusters,
        )
        cleanup_databricks_workflow_task: CleanupDatabricksWorkflowOperator = (
            CleanupDatabricksWorkflowOperator(
                dag=self.dag,
                task_id="cleanup",
                databricks_conn_id=self.databricks_conn_id,
                job_cluster_ids=[c["cluster_id"] for c in self.job_clusters],
                run_id=create_databricks_workflow_task.output,
            )
        )

        for task in roots:
            if not (
                hasattr(task, "convert_to_databricks_workflow_task")
                and callable(task.convert_to_databricks_workflow_task)
            ):
                raise AirflowException(
                    f"Task {task.task_id} does not support conversion to databricks workflow task."
                )
            create_databricks_workflow_task.set_upstream(task_or_task_list=list(task.upstream_list))
        for task in self.leaves:
            cleanup_databricks_workflow_task.set_downstream(task_or_task_list=list(task.downstream_list))

        for task_id, task in self.children.items():
            if task_id != f"{self.group_id}.launch":
                create_databricks_workflow_task.relevant_upstreams.append(task_id)
                create_databricks_workflow_task.add_task(task)
                task.databricks_run_id = create_databricks_workflow_task.output

        create_databricks_workflow_task.set_downstream(roots)
        super().__exit__(_type, _value, _tb)
