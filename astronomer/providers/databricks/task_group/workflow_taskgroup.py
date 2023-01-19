from __future__ import annotations

from logging import Logger
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass
from typing import Any

from airflow.exceptions import AirflowException
from airflow.models.operator import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient


class CreateDatabricksWorkflowOperator(BaseOperator):
    def __init__(
        self,
        task_id,
        databricks_conn_id,
        job_cluster_json: dict[str, object] = None,
        tasks_to_convert: list[BaseOperator] = None,
        **kwargs,
    ):
        self.job_cluster_key = job_cluster_json["job_cluster_key"]
        self.job_cluster_json = job_cluster_json
        self.tasks_to_convert = tasks_to_convert or []
        self.relevant_upstreams = [task_id]
        self.databricks_conn_id = databricks_conn_id
        self.databricks_run_id = None
        super().__init__(task_id=task_id, **kwargs)

    def add_task(self, task: BaseOperator):
        self.tasks_to_convert.append(task)

    def execute(self, context: Context) -> Any:

        task_json = [task.convert_to_databricks_workflow_task() for task in self.tasks_to_convert]
        full_json = {
            "name": "airflow-job-test",
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": task_json,
            "job_clusters": [self.job_cluster_json],
            "format": "MULTI_TASK",
        }
        hook = DatabricksHook(self.databricks_conn_id)
        databricks_conn = hook.get_conn()
        api_client = ApiClient(
            user=databricks_conn.login, password=databricks_conn.password, host=databricks_conn.host
        )
        jobs_api = JobsApi(api_client=api_client)
        job_id = jobs_api.create_job(json=full_json)
        run_info = jobs_api.run_now(
            job_id=job_id["job_id"],
            jar_params=None,
            python_params=None,
            notebook_params=None,
            spark_submit_params=None,
        )
        run_id = run_info["run_id"]
        runs_api = RunsApi(api_client)
        import time

        while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "PENDING":
            print("job pending")
            time.sleep(5)
        return run_id


class DatabricksWorkflowTaskGroup(TaskGroup):
    @property
    def log(self) -> Logger:
        pass

    is_databricks = True

    def __init__(self, job_cluster_json, databricks_conn_id, **kwargs):
        self.databricks_conn_id = databricks_conn_id
        self.job_cluster_json = job_cluster_json
        super().__init__(**kwargs)

    def __exit__(self, _type, _value, _tb):
        roots = self.roots
        create_databricks_workflow_task: CreateDatabricksWorkflowOperator = CreateDatabricksWorkflowOperator(
            dag=self.dag,
            task_id="launch",
            databricks_conn_id=self.databricks_conn_id,
            job_cluster_json=self.job_cluster_json,
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
