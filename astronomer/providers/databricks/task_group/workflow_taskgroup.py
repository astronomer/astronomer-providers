from __future__ import annotations

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
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient

from astronomer.providers.utils.typing_compat import Context


def _get_job_by_name(job_name: str, jobs_api: JobsApi) -> dict | None:
    jobs = jobs_api.list_jobs().get("jobs", [])
    for job in jobs:
        if job.get("settings", {}).get("name") == job_name:
            return job
    return None


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

    def create_workflow_json(self) -> dict[str, object]:
        """
        Creates a workflow json that can be submitted to databricks
        :return:
        """
        task_json = [
            task.convert_to_databricks_workflow_task(relevant_upstreams=self.relevant_upstreams)
            for task in self.tasks_to_convert
        ]
        full_json = {
            "name": self.databricks_job_name,
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": task_json,
            "format": "MULTI_TASK",
            "job_clusters": self.job_clusters,
        }
        return full_json

    @property
    def databricks_job_name(self):
        return self.dag_id + "." + self.task_group.group_id

    def execute(self, context: Context) -> Any:
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        api_client = ApiClient(token=databricks_conn.password, host=databricks_conn.host)
        jobs_api = JobsApi(api_client)
        job = _get_job_by_name(self.databricks_job_name, jobs_api)
        job_id = job["job_id"] if job else None
        current_job_spec = self.create_workflow_json()
        if not isinstance(self.task_group, DatabricksWorkflowTaskGroup):
            raise AirflowException("Task group must be a DatabricksWorkflowTaskGroup")
        if job_id:
            jobs_api.reset_job(json={"job_id": job_id, "new_settings": current_job_spec})
        else:
            job_id = jobs_api.create_job(json=current_job_spec)["job_id"]
        run_id = jobs_api.run_now(
            job_id=job_id,
            jar_params=self.task_group.jar_params,
            notebook_params=self.task_group.notebook_params,
            python_params=self.task_group.python_params,
            spark_submit_params=self.task_group.spark_submit_params,
        )["run_id"]
        runs_api = RunsApi(api_client)

        while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "PENDING":
            print("job pending")
            time.sleep(5)
        return run_id


class DatabricksWorkflowTaskGroup(TaskGroup):
    @property
    def log(self) -> Logger:
        pass

    is_databricks = True

    def __init__(
        self,
        databricks_conn_id,
        existing_clusters=None,
        job_clusters=None,
        jar_params: dict = None,
        notebook_params: list = None,
        python_params: list = None,
        spark_submit_params: list = None,
        **kwargs,
    ):
        self.databricks_conn_id = databricks_conn_id
        self.existing_clusters = existing_clusters or []
        self.job_clusters = job_clusters or []
        self.notebook_params = notebook_params or []
        self.python_params = python_params or []
        self.spark_submit_params = spark_submit_params or []
        self.jar_params = jar_params or []
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

        for task in roots:
            if not (
                hasattr(task, "convert_to_databricks_workflow_task")
                and callable(task.convert_to_databricks_workflow_task)
            ):
                raise AirflowException(
                    f"Task {task.task_id} does not support conversion to databricks workflow task."
                )
            create_databricks_workflow_task.set_upstream(task_or_task_list=list(task.upstream_list))

        for task_id, task in self.children.items():
            if task_id != f"{self.group_id}.launch":
                create_databricks_workflow_task.relevant_upstreams.append(task_id)
                create_databricks_workflow_task.add_task(task)
                task.databricks_run_id = create_databricks_workflow_task.output

        create_databricks_workflow_task.set_downstream(roots)
        super().__exit__(_type, _value, _tb)
