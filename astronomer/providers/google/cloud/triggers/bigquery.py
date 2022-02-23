import asyncio
from typing import Any, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.google.cloud.hooks.bigquery import BigQueryHookAsync


class BigQueryInsertJobTrigger(BaseTrigger):
    def __init__(
        self,
        conn_id: str,
        job_id: Optional[str],
        project_id: Optional[str],
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        poll_interval: float = 4.0,
    ):
        self.log.info("Using the connection  %s .", conn_id)
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
        Serializes BigQueryInsertJobTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryInsertJobTrigger",
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
        """
        Gets current job execution status and yields a TriggerEvent
        """
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                self.log.debug("Response from hook: %s", response_from_hook)

                if response_from_hook == "success":
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                    return
                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
                    return

            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return

    def _get_async_hook(self) -> BigQueryHookAsync:
        return BigQueryHookAsync(gcp_conn_id=self.conn_id)


class BigQueryCheckTrigger(BigQueryInsertJobTrigger):
    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes BigQueryCheckTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryCheckTrigger",
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
        """
        Gets current job execution status and yields a TriggerEvent
        """
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    # Extract records after casting a BigQuery row to the appropriate data types.
                    records = hook.get_records(query_results, nocast=False)
                    # If empty list, then no records are available
                    if not records:
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "records": None,
                            }
                        )
                    else:
                        # Extract only first record from the query results
                        first_record = records.pop(0)
                        yield TriggerEvent(
                            {
                                "status": "success",
                                "records": first_record,
                            }
                        )
                    return

                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
                    return
            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return


class BigQueryGetDataTrigger(BigQueryInsertJobTrigger):
    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes BigQueryInsertJobTrigger arguments and classpath.
        """
        return (
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryGetDataTrigger",
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
        """
        Gets current job execution status and yields a TriggerEvent with response data
        """
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    records = hook.get_records(query_results)
                    self.log.debug("Response from hook: %s", response_from_hook)
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": response_from_hook,
                            "records": records,
                        }
                    )
                    return
                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})
                    return
            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return
