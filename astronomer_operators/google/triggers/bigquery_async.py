import asyncio
from typing import Any, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.google.hooks.bigquery_async import BigQueryHookAsync


class BigQueryTrigger(BaseTrigger):
    def __init__(
        self,
        conn_id: str,
        job_id: str,
        dataset_id: str,
        project_id: Optional[str],
        table_id: str,
        poll_interval: float = 2.0,
    ):
        print("In BigQueryTrigger")
        print("conn_id", conn_id)
        print("job_id", job_id)
        super().__init__()
        self.conn_id = conn_id
        self.job_id = job_id
        self._job_conn = None
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes BigQueryTrigger arguments and classpath.
        """
        print("serializing the trigger...")
        return (
            "astronomer_operators.google.triggers.bigquery_async.BigQueryTrigger",
            {
                "conn_id": self.conn_id,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        print("In trigger run method")
        hook = self._get_async_hook()
        print(self.job_id)
        print(self.project_id)
        while True:
            try:
                print("This is test trigger")
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                self.log.debug("Response from hook: %s", response_from_hook)
                print(response_from_hook)

                if response_from_hook["jobComplete"]:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Query completed",
                            "api_response": response_from_hook["rows"],
                        }
                    )
                    return

                # When the query is still running, "jobComplete" will be False
                self.log.info("Query is still running...")
                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)
            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e), "api_response": None})
                return

    def _get_async_hook(self) -> BigQueryHookAsync:
        print("In _get_async_hook")
        return BigQueryHookAsync(gcp_conn_id=self.conn_id)
