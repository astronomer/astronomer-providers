import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

from airflow import AirflowException
from airflow.triggers.base import TriggerEvent

from astronomer.providers.http.triggers.http import HttpTrigger


class ExternalDeploymentDagRunTrigger(HttpTrigger):
    """Inherits from HttpTrigger and make Async http call to get the Dag Run state."""

    def __init__(self, dag_id: str, run_id: str, endpoint: str, **kwargs):
        super().__init__(endpoint, **kwargs)
        self.dag_id = dag_id
        self.run_id = run_id

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serialize ExternalDeploymentDagRunTrigger arguments and classpath."""
        return (
            "astronomer.providers.core.triggers.external_dagrun.ExternalDeploymentDagRunTrigger",
            {
                "endpoint": self.endpoint,
                "data": self.data,
                "headers": self.headers,
                "extra_options": self.extra_options,
                "http_conn_id": self.http_conn_id,
                "poke_interval": self.poke_interval,
                "dag_id": self.dag_id,
                "run_id": self.run_id,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """Make a series of asynchronous http calls via a http hook poll for state of the dag run
        until it reaches a failure state or success state. It yields a Trigger if response state is successful.
        """
        from airflow.utils.state import State

        hook = self._get_async_hook()

        while True:
            self.log.info(f"Waiting for Dag {self.dag_id} run {self.run_id} to complete.")

            try:
                response = await hook.run(
                    endpoint=self.endpoint,
                    data=self.data,
                    headers=self.headers,
                    extra_options=self.extra_options,
                )
                resp_json = await response.json()
                if resp_json["state"] in State.finished:
                    yield TriggerEvent(resp_json)
                else:
                    await asyncio.sleep(self.poke_interval)
            except AirflowException as exc:
                if str(exc).startswith("404"):
                    await asyncio.sleep(self.poke_interval)
                yield TriggerEvent({"state": "error", "message": str(exc)})
