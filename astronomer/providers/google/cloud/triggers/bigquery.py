import asyncio
from typing import (
    Any,
    AsyncIterator,
    Dict,
    Optional,
    Sequence,
    SupportsAbs,
    Tuple,
    Union,
)

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientResponseError
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.google.cloud.hooks.bigquery import (
    BigQueryHookAsync,
    BigQueryTableHookAsync,
)


class BigQueryInsertJobTrigger(BaseTrigger):
    """
    BigQueryInsertJobTrigger run on the trigger worker to perform insert operation

    :param conn_id: Reference to google cloud connection id
    :param job_id:  The ID of the job. It will be suffixed with hash of job configuration
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param delegate_to: This performs a task on one host with reference to other hosts.
    :param impersonation_chain: This is the optional service account to impersonate using short term
        credentials.
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        conn_id: str,
        job_id: Optional[str],
        project_id: Optional[str],
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
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
                "delegate_to": self.delegate_to,
                "impersonation_chain": self.impersonation_chain,
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
                            "job_id": self.job_id,
                            "status": "success",
                            "message": "Job completed",
                        }
                    )
                elif response_from_hook == "pending":
                    self.log.info("Query is still running...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": response_from_hook})

            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})

    def _get_async_hook(self) -> BigQueryHookAsync:
        return BigQueryHookAsync(
            gcp_conn_id=self.conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )


class BigQueryCheckTrigger(BigQueryInsertJobTrigger):
    """BigQueryCheckTrigger run on the trigger worker"""

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
                "impersonation_chain": self.impersonation_chain,
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

                    records = hook.get_records(query_results)

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
            except Exception as e:
                self.log.exception("Exception occurred while checking for query completion")
                yield TriggerEvent({"status": "error", "message": str(e)})


class BigQueryGetDataTrigger(BigQueryInsertJobTrigger):
    """BigQueryGetDataTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class"""

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
                "delegate_to": self.delegate_to,
                "impersonation_chain": self.impersonation_chain,
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


class BigQueryIntervalCheckTrigger(BigQueryInsertJobTrigger):
    """
    BigQueryIntervalCheckTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class

    :param conn_id: Reference to google cloud connection id
    :param first_job_id:  The ID of the job 1 performed
    :param second_job_id:  The ID of the job 2 performed
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table: table name
    :param metrics_thresholds: dictionary of ratios indexed by metrics
    :param date_filter_column: column name
    :param days_back: number of days between ds and the ds we want to check
        against
    :param ratio_formula: ration formula
    :param ignore_zero: boolean value to consider zero or not
    :param table_id: The table ID of the requested table. (templated)
    :param impersonation_chain: This is the optional service account to impersonate using short term
        credentials.
    :param poll_interval: polling period in seconds to check for the status
    """

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
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        poll_interval: float = 4.0,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=first_job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            impersonation_chain=impersonation_chain,
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

                    first_records = hook.get_records(first_query_results)

                    second_records = hook.get_records(second_query_results)

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


class BigQueryValueCheckTrigger(BigQueryInsertJobTrigger):
    """
    BigQueryValueCheckTrigger run on the trigger worker, inherits from BigQueryInsertJobTrigger class

    :param conn_id: Reference to google cloud connection id
    :param sql: the sql to be executed
    :param pass_value: pass value
    :param job_id:  The ID of the job
    :param project_id: Google Cloud Project where the job is running
    :param tolerance: certain metrics for tolerance
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param impersonation_chain: This is the optional service account to impersonate using short term
        credentials.
    :param poll_interval: polling period in seconds to check for the status
    """

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
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        poll_interval: float = 4.0,
    ):
        super().__init__(
            conn_id=conn_id,
            job_id=job_id,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            impersonation_chain=impersonation_chain,
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
                    records = hook.get_records(query_results)
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


class BigQueryTableExistenceTrigger(BaseTrigger):
    """
    Initialise the BigQuery Table Existence Trigger with needed parameters

    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table.
    :param table_id: The table ID of the requested table.
    :param gcp_conn_id: Reference to google cloud connection id
    :param hook_params: params for hook
    :param poke_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        gcp_conn_id: str,
        hook_params: Dict[str, Any],
        poke_interval: float = 4.0,
    ):
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.gcp_conn_id: str = gcp_conn_id
        self.poke_interval = poke_interval
        self.hook_params = hook_params

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes BigQueryTableExistenceTrigger arguments and classpath."""
        return (
            "astronomer.providers.google.cloud.triggers.bigquery.BigQueryTableExistenceTrigger",
            {
                "dataset_id": self.dataset_id,
                "project_id": self.project_id,
                "table_id": self.table_id,
                "gcp_conn_id": self.gcp_conn_id,
                "poke_interval": self.poke_interval,
                "hook_params": self.hook_params,
            },
        )

    def _get_async_hook(self) -> BigQueryTableHookAsync:
        return BigQueryTableHookAsync(gcp_conn_id=self.gcp_conn_id, **self.hook_params)

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Will run until the table exists in the Google Big Query."""
        while True:
            try:
                hook = self._get_async_hook()
                response = await self._table_exists(
                    hook=hook, dataset=self.dataset_id, table_id=self.table_id, project_id=self.project_id
                )
                if response:
                    yield TriggerEvent({"status": "success", "message": "success"})
                    return
                await asyncio.sleep(self.poke_interval)
            except Exception as e:
                self.log.exception("Exception occurred while checking for Table existence")
                yield TriggerEvent({"status": "error", "message": str(e)})
                return

    async def _table_exists(
        self, hook: BigQueryTableHookAsync, dataset: str, table_id: str, project_id: str
    ) -> bool:
        """
        Create client session and make call to BigQueryTableHookAsync and check for the table in Google Big Query.

        :param hook: BigQueryTableHookAsync Hook class
        :param dataset:  The name of the dataset in which to look for the table storage bucket.
        :param table_id: The name of the table to check the existence of.
        :param project_id: The Google cloud project in which to look for the table.
            The connection supplied to the hook must provide
            access to the specified project.
        """
        async with ClientSession() as session:
            try:
                client = await hook.get_table_client(
                    dataset=dataset, table_id=table_id, project_id=project_id, session=session
                )
                response = await client.get()
                return True if response else False
            except ClientResponseError as err:
                if err.status == 404:
                    return False
                raise err
