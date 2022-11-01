import asyncio
from typing import (
    Any,
    AsyncIterator,
    Collection,
    Dict,
    Optional,
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


class PongTrigger(BaseTrigger):
    """Trigger to implement a "ping-pong" effect."""

    serialize_fields: Collection[str]
    poll_interval: float

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes trigger arguments and classpath."""
        return (
            f"{self.__module__}.{self.__class__.__name__}",
            {f: getattr(self, f) for f in self.serialize_fields},
        )

    async def poll_process(self) -> Tuple[Optional[str], Any]:
        """Override to implement polling.

        This should implement only *one* polling round; the triggerer acts as
        a loop to call this repeatedly. Do not catch exceptions in this
        function; they are caught by the caller to emit an error event.

        :return: A two-tuple representing the status, and additional payload to
            include in the pong message. The status should be either "success",
            "pending", or "error"; any other values would be coerced in to
            "error" in the pong message.
        """
        raise NotImplementedError

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        await asyncio.sleep(self.poll_interval)

        _, data = self.serialize()
        try:
            response, result = await self.poll_process()
        except Exception as e:
            self.log.exception("Exception occurred while checking for query completion")
            yield TriggerEvent({"status": "error", "message": str(e), "trigger": data})
            return

        if response == "success":
            self.log.debug("Poll response: %s", response)
            yield TriggerEvent({"status": "success", "trigger": data, "result": result})
        elif response == "pending":
            self.log.debug("Still pending... sleeping for %s seconds.", self.poll_interval)
            yield TriggerEvent({"status": "pending", "trigger": data, "result": result})
        elif response == "error":
            self.log.error("Poll response: %s", response)
            message = "Error response from hook"
            yield TriggerEvent({"status": "error", "message": message, "trigger": data, "result": result})
        else:
            self.log.error("Unknown poll response: %s", response)
            message = f"Unknown response from hook: {response}"
            yield TriggerEvent({"status": "error", "message": message, "trigger": data, "result": result})


class BigQueryTrigger(PongTrigger):
    """BigQueryTrigger run on the trigger worker to perform an operation.

    :param conn_id: Reference to google cloud connection id
    :param job_id:  The ID of the job. It will be suffixed with hash of job configuration
    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param poll_interval: polling period in seconds to check for the status
    """

    serialize_fields = ["conn_id", "job_id", "dataset_id", "project_id", "table_id", "poll_interval"]

    def __init__(
        self,
        *,
        conn_id: str,
        job_id: Optional[str],
        project_id: Optional[str],
        dataset_id: Optional[str] = None,
        table_id: Optional[str] = None,
        poll_interval: float = 4.0,
    ) -> None:
        super().__init__()
        self.log.info("Using connection %r.", conn_id)
        self.conn_id = conn_id
        self.job_id = job_id
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.poll_interval = poll_interval

    def _get_async_hook(self) -> BigQueryHookAsync:
        return BigQueryHookAsync(gcp_conn_id=self.conn_id)


class BigQueryInsertJobTrigger(BigQueryTrigger):
    """BigQueryInsertJobTrigger run on the trigger worker"""

    async def poll_process(self) -> Tuple[Optional[str], Any]:
        """Poll the Big Query job."""
        hook = self._get_async_hook()
        response = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
        return response, None


class BigQueryGetDataTrigger(BigQueryTrigger):
    """BigQueryGetDataTrigger run on the trigger worker"""

    async def poll_process(self) -> Tuple[Optional[str], Any]:
        """Poll the Big Query job."""
        hook = self._get_async_hook()
        response = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
        if response != "success":
            return response, None
        query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
        return "success", hook.get_records(query_results)


class BigQueryIntervalCheckTrigger(BigQueryTrigger):
    """
    BigQueryIntervalCheckTrigger run on the trigger worker, inherits from BigQueryTrigger.

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
    :param poll_interval: polling period in seconds to check for the status
    """

    serialize_fields = [
        "conn_id",
        "first_job_id",
        "second_job_id",
        "project_id",
        "table",
        "metrics_thresholds",
        "date_filter_column",
        "days_back",
        "ratio_formula",
        "ignore_zero",
        "poll_interval",
    ]

    def __init__(
        self,
        *,
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
        self.first_job_id = first_job_id
        self.second_job_id = second_job_id
        self.table = table
        self.metrics_thresholds = metrics_thresholds
        self.date_filter_column = date_filter_column
        self.days_back = days_back
        self.ratio_formula = ratio_formula
        self.ignore_zero = ignore_zero

    async def poll_process(self) -> Tuple[Optional[str], Any]:
        """Poll the two jobs and combine their results."""
        hook = self._get_async_hook()
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

            return "success", [first_job_row, second_job_row]
        elif first_job_response_from_hook == "pending" or second_job_response_from_hook == "pending":
            self.log.info("Query is still running...")
            self.log.info("Sleeping for %s seconds.", self.poll_interval)
            return "pending", None

        return "error", [first_job_response_from_hook, second_job_response_from_hook]


class BigQueryValueCheckTrigger(BigQueryTrigger):
    """
    BigQueryValueCheckTrigger run on the trigger worker, inherits from BigQueryTrigger.

    :param conn_id: Reference to google cloud connection id
    :param sql: the sql to be executed
    :param pass_value: pass value
    :param job_id:  The ID of the job
    :param project_id: Google Cloud Project where the job is running
    :param tolerance: certain metrics for tolerance
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param poll_interval: polling period in seconds to check for the status
    """

    serialize_fields = [
        "conn_id",
        "job_id",
        "dataset_id",
        "project_id",
        "table_id",
        "sql",
        "pass_value",
        "tolerance",
        "poll_interval",
    ]

    def __init__(
        self,
        *,
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

    async def poll_process(self) -> Tuple[Optional[str], Any]:
        """Poll the Big Query job."""
        hook = self._get_async_hook()
        response = await hook.get_job_status(job_id=self.job_id, project_id=self.project_id)
        if response == "success":
            query_results = await hook.get_job_output(job_id=self.job_id, project_id=self.project_id)
            try:
                record = hook.get_records(query_results)[0]
            except IndexError:
                record = None
            hook.value_check(self.sql, self.pass_value, record, self.tolerance)
            return "success", record
        return response, None


class BigQueryTableExistenceTrigger(PongTrigger):
    """
    Initialise the BigQuery Table Existence Trigger with needed parameters

    :param project_id: Google Cloud Project where the job is running
    :param dataset_id: The dataset ID of the requested table.
    :param table_id: The table ID of the requested table.
    :param gcp_conn_id: Reference to google cloud connection id
    :param hook_params: params for hook
    :param poke_interval: polling period in seconds to check for the status
    """

    serialize_fields = [
        "conn_id",
        "project_id",
        "dataset_id",
        "table_id",
        "hook_params",
        "poll_interval",
    ]

    def __init__(
        self,
        *,
        conn_id: str,
        project_id: str,
        dataset_id: str,
        table_id: str,
        hook_params: Dict[str, Any],
        poll_interval: float = 4.0,
    ):
        self.conn_id = conn_id
        self.dataset_id = dataset_id
        self.project_id = project_id
        self.table_id = table_id
        self.hook_params = hook_params
        self.poll_interval = poll_interval

    def _get_async_hook(self) -> BigQueryTableHookAsync:
        return BigQueryTableHookAsync(gcp_conn_id=self.conn_id)

    async def _table_exists(self) -> bool:
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
                client = await self._get_async_hook().get_table_client(
                    dataset=self.dataset_id,
                    table_id=self.table_id,
                    project_id=self.project_id,
                    session=session,
                )
                response = await client.get()
                return bool(response)
            except ClientResponseError as err:
                if err.status == 404:
                    return False
                raise

    async def poll_process(self) -> Tuple[Optional[str], Any]:
        """Poll the Big Query job."""
        result = await self._table_exists()
        if result:
            return "success", None
        return "pending", None
