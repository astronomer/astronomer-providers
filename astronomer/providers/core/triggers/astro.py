from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.core.hooks.astro import AstroHook


class AstroDeploymentTrigger(BaseTrigger):
    """
    Custom Apache Airflow trigger for monitoring the completion status of an external deployment using Astro Cloud.

    :param external_dag_id: External ID of the DAG being monitored.
    :param dag_run_id: ID of the DAG run being monitored.
    :param external_task_id: External ID of the task being monitored. If None, monitors the entire DAG.
    :param astro_cloud_conn_id: The connection ID to retrieve Astro Cloud credentials. Defaults to "astro_cloud_default".
    :param poke_interval: Time in seconds to wait between consecutive checks for completion status.
    :param kwargs: Additional keyword arguments passed to the BaseTrigger constructor.
    """

    def __init__(
        self,
        external_dag_id: str,
        dag_run_id: str,
        external_task_id: str | None = None,
        astro_cloud_conn_id: str = "astro_cloud_default",
        poke_interval: float = 5.0,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.external_dag_id = external_dag_id
        self.dag_run_id = dag_run_id
        self.external_task_id = external_task_id
        self.astro_cloud_conn_id = astro_cloud_conn_id
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for storage in the database."""
        return (
            "astronomer.providers.core.triggers.astro.AstroDeploymentTrigger",
            {
                "external_dag_id": self.external_dag_id,
                "external_task_id": self.external_task_id,
                "dag_run_id": self.dag_run_id,
                "astro_cloud_conn_id": self.astro_cloud_conn_id,
                "poke_interval": self.poke_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Asynchronously runs the trigger and yields completion status events.

        Checks the status of the external deployment using Astro Cloud at regular intervals.
        Yields TriggerEvent with the status "done" if successful, "failed" if failed.
        """
        hook = AstroHook(self.astro_cloud_conn_id)
        while True:
            if self.external_task_id is not None:
                task_instance = await hook.get_a_task_instance(
                    self.external_dag_id, self.dag_run_id, self.external_task_id
                )
                state = task_instance.get("state") if task_instance else None
                if state in ("success", "skipped"):
                    yield TriggerEvent({"status": "done"})
                elif state in ("failed", "upstream_failed"):
                    yield TriggerEvent({"status": "failed"})
            else:
                dag_run = await hook.get_a_dag_run(self.external_dag_id, self.dag_run_id)
                state = dag_run.get("state") if dag_run else None
                if state == "success":
                    yield TriggerEvent({"status": "done"})
                elif state == "failed":
                    yield TriggerEvent({"status": "failed"})
            self.log.info("Job status is %s sleeping for %s seconds.", state, self.poke_interval)
            await asyncio.sleep(self.poke_interval)
