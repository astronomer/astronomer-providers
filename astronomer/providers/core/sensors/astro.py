from __future__ import annotations

import datetime
from typing import Any, cast

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.sensors.base import BaseSensorOperator, PokeReturnValue

from astronomer.providers.core.hooks.astro import AstroHook
from astronomer.providers.core.triggers.astro import AstroDeploymentTrigger
from astronomer.providers.utils.typing_compat import Context


class ExternalDeploymentSensor(BaseSensorOperator):
    """
    Custom Apache Airflow sensor for monitoring external deployments using Astro Cloud.

    :param external_dag_id: External ID of the DAG being monitored.
    :param astro_cloud_conn_id: The connection ID to retrieve Astro Cloud credentials.
                               Defaults to "astro_cloud_default".
    :param external_task_id: External ID of the task being monitored. If None, monitors the entire DAG.
    :param kwargs: Additional keyword arguments passed to the BaseSensorOperator constructor.
    """

    def __init__(
        self,
        external_dag_id: str,
        astro_cloud_conn_id: str = "astro_cloud_default",
        external_task_id: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.astro_cloud_conn_id = astro_cloud_conn_id
        self.external_task_id = external_task_id
        self.external_dag_id = external_dag_id
        self._dag_run_id: str = ""

    def poke(self, context: Context) -> bool | PokeReturnValue:
        """
        Check the status of a DAG/task in another deployment.

        Queries Airflow's REST API for the status of the specified DAG or task instance.
        Returns True if successful, False otherwise.

        :param context: The task execution context.
        """
        hook = AstroHook(self.astro_cloud_conn_id)
        dag_runs: list[dict[str, Any]] = hook.get_dag_runs(self.external_dag_id)
        if not dag_runs:
            self.log.info("No DAG runs found for DAG %s", self.external_dag_id)
            return True
        self._dag_run_id = cast(str, dag_runs[0]["dag_run_id"])
        if self.external_task_id is not None:
            task_instance = hook.get_task_instance(
                self.external_dag_id, self._dag_run_id, self.external_task_id
            )
            task_state = task_instance.get("state") if task_instance else None
            if task_state == "success":
                return True
        else:
            state = dag_runs[0].get("state")
            if state == "success":
                return True
        return False

    def execute(self, context: Context) -> Any:
        """
        Executes the sensor.

        If the external deployment is not successful, it defers the execution using an AstroDeploymentTrigger.

        :param context: The task execution context.
        """
        if not self.poke(context):
            self.defer(
                timeout=datetime.timedelta(seconds=self.timeout),
                trigger=AstroDeploymentTrigger(
                    astro_cloud_conn_id=self.astro_cloud_conn_id,
                    external_task_id=self.external_task_id,
                    external_dag_id=self.external_dag_id,
                    poke_interval=self.poke_interval,
                    dag_run_id=self._dag_run_id,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:
        """
        Handles the completion event from the deferred execution.

        Raises AirflowSkipException if the upstream job failed and `soft_fail` is True.
        Otherwise, raises AirflowException.

        :param context: The task execution context.
        :param event: The event dictionary received from the deferred execution.
        """
        if event.get("status") == "failed":
            if self.soft_fail:
                raise AirflowSkipException("Upstream job failed. Skipping the task.")
            raise AirflowException("Upstream job failed.")
