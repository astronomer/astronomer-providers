import asyncio
from typing import Any, AsyncIterator, Dict, Optional, SupportsAbs, Tuple, Union

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.google.cloud.hooks.bigquery import BigQueryHookAsync


class BigQueryInsertJobTrigger(BaseTrigger):  # noqa: D101
    def __init__(
        self,
        conn_id: str,
        job_id: Optional[str],
        project_id: Optional[str],
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        poll_interval: float = 4.0,
    ):
        super().__init__()
        self.log.info("Using the connection  %s .", conn_id)
        self.conn_id = conn_id
        self.job_id = job_id
        self._job_conn = None
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BigQueryInsertJobTrigger arguments and classpath."""
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

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
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


class BigQueryCheckTrigger(BigQueryInsertJobTrigger):  # noqa: D101
    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BigQueryCheckTrigger arguments and classpath."""
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

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
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


class BigQueryGetDataTrigger(BigQueryInsertJobTrigger):  # noqa: D101
    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BigQueryInsertJobTrigger arguments and classpath."""
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

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent with response data"""
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


class BigQueryIntervalCheckTrigger(BigQueryInsertJobTrigger):  # noqa: D101
    def __init__(
        self,
        conn_id: str,
        first_job_id: str,
        second_job_id: str,
        project_id: Optional[str],
        table: str,
        metrics_thresholds: Dict[str, int],
        date_filter_column: Optional[str] = "ds",
        days_back: SupportsAbs[int] = -7,
        ratio_formula: str = "max_over_min",
        ignore_zero: bool = True,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        poll_interval: float = 4.0,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=first_job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            poll_interval=poll_interval,
        )
        self.conn_id = conn_id
        self.first_job_id = first_job_id
        self.second_job_id = second_job_id
        self.project_id = project_id
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.date_filter_column = date_filter_column
        self.days_back = days_back
        self.ratio_formula = ratio_formula
        self.ignore_zero = ignore_zero

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BigQueryCheckTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryIntervalCheckTrigger",
            {
                "conn_id": self.conn_id,
                "first_job_id": self.first_job_id,
                "second_job_id": self.second_job_id,
                "project_id": self.project_id,
                "table": self.table,
                "metrics_thresholds": self.metrics_thresholds,
                "date_filter_column": self.date_filter_column,
                "days_back": self.days_back,
                "ratio_formula": self.ratio_formula,
                "ignore_zero": self.ignore_zero,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        while True:
            try:
                first_job_response_from_hook = await hook.get_job_status(
                    job_id=self.first_job_id, project_id=self.project_id
                )
                second_job_response_from_hook = await hook.get_job_status(
                    job_id=self.second_job_id, project_id=self.project_id
                )

                if first_job_response_from_hook == "success" and second_job_response_from_hook == "success":
                    first_query_results = await hook.get_job_output(
                        job_id=self.first_job_id, project_id=self.project_id
                    )

                    second_query_results = await hook.get_job_output(
                        job_id=self.second_job_id, project_id=self.project_id
                    )

                    # Extract records after casting a BigQuery row to the appropriate data types.
                    first_records = hook.get_records(first_query_results, nocast=False)

                    # Extract records after casting a BigQuery row to the appropriate data types.
                    second_records = hook.get_records(second_query_results, nocast=False)

                    # If empty list, then no records are available
                    if not first_records:
                        first_job_row: Optional[str] = None
                    else:
                        # Extract only first record from the query results
                        first_job_row = first_records.pop(0)

                    # If empty list, then no records are available
                    if not second_records:
                        second_job_row: Optional[str] = None
                    else:
                        # Extract only first record from the query results
                        second_job_row = second_records.pop(0)

                    hook.interval_check(
                        first_job_row,
                        second_job_row,
                        self.metrics_thresholds,
                        self.ignore_zero,
                        self.ratio_formula,
                    )

                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Job completed",
                            "first_row_data": first_job_row,
                            "second_row_data": second_job_row,
                        }
                    )
                    return
                elif first_job_response_from_hook == "pending" or second_job_response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent(
                        {"status": "error", "message": second_job_response_from_hook, "data": None}
                    )
                    return

            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return


class BigQueryValueCheckTrigger(BigQueryInsertJobTrigger):  # noqa: D101
    def __init__(
        self,
        conn_id: str,
        sql: str,
        pass_value: Union[int, float, str],
        job_id: Optional[str],
        project_id: Optional[str],
        tolerance: Any = None,
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        poll_interval: float = 4.0,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            poll_interval=poll_interval,
        )
        self.sql = sql
        self.pass_value = pass_value
        self.tolerance = tolerance

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BigQueryValueCheckTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryValueCheckTrigger",
            {
                "conn_id": self.conn_id,
                "pass_value": self.pass_value,
                "job_id": self.job_id,
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "sql": self.sql,
                "table_id": self.table_id,
                "tolerance": self.tolerance,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        hook = self._get_async_hook()
        while True:
            try:
                # Poll for job execution status
                response_from_hook = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
                if response_from_hook == "success":
                    query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
                    # Extract records after casting a BigQuery row to the appropriate data types.
                    records = hook.get_records(query_results, nocast=False)
                    records = records.pop(0) if records else None
                    hook.value_check(self.sql, self.pass_value, records, self.tolerance)
                    yield TriggerEvent({"status": "success", "message": "Job completed", "records": records})
                    return
                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook, "records": None})
                    return

            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return
