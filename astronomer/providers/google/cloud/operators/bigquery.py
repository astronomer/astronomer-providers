"""This module contains Google BigQueryAsync providers."""
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)
from google.api_core.exceptions import Conflict

from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryCheckTrigger,
    BigQueryGetDataTrigger,
    BigQueryInsertJobTrigger,
    BigQueryIntervalCheckTrigger,
    BigQueryValueCheckTrigger,
)
from astronomer.providers.utils.typing_compat import Context

BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"


class BigQueryInsertJobOperatorAsync(BigQueryInsertJobOperator, BaseOperator):
    """
    Starts a BigQuery job asynchronously, and returns job id.
    This operator works in the following way:

    - it calculates a unique hash of the job using job's configuration or uuid if ``force_rerun`` is True
    - creates ``job_id`` in form of
        ``[provided_job_id | airflow_{dag_id}_{task_id}_{exec_date}]_{uniqueness_suffix}``
    - submits a BigQuery job using the ``job_id``
    - if job with given id already exists then it tries to reattach to the job if its not done and its
        state is in ``reattach_states``. If the job is done the operator will raise ``AirflowException``.

    Using ``force_rerun`` will submit a new job every time without attaching to already existing ones.

    For job definition see here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

    :param configuration: The configuration parameter maps directly to BigQuery's
        configuration field in the job  object. For more details see
        https://cloud.google.com/bigquery/docs/reference/v2/jobs
    :param job_id: The ID of the job. It will be suffixed with hash of job configuration
        unless ``force_rerun`` is True.
        The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
        dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
        be generated.
    :param force_rerun: If True then operator will use hash of uuid as job id suffix
    :param reattach_states: Set of BigQuery job's states in case of which we should reattach
        to the job. Should be other than final states.
    :param project_id: Google Cloud Project where the job is running
    :param location: location the job is running
    :param gcp_conn_id: The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called
    """

    def execute(self, context: Context) -> None:  # noqa: D102
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.hook = hook
        job_id = self.hook.generate_job_id(
            job_id=self.job_id,
            dag_id=self.dag_id,
            task_id=self.task_id,
            logical_date=context["logical_date"],
            configuration=self.configuration,
            force_rerun=self.force_rerun,
        )

        try:
            job = self._submit_job(hook, job_id)
            self._handle_job_error(job)
        except Conflict:
            # If the job already exists retrieve it
            job = hook.get_job(
                project_id=self.project_id,
                location=self.location,
                job_id=job_id,
            )
            if job.state in self.reattach_states:
                # We are reattaching to a job
                job._begin()
                self._handle_job_error(job)
            else:
                # Same job configuration so we need force_rerun
                raise AirflowException(
                    f"Job with id: {job_id} already exists and is in {job.state} state. If you "
                    f"want to force rerun it consider setting `force_rerun=True`."
                    f"Or, if you want to reattach in this scenario add {job.state} to `reattach_states`"
                )

        self.job_id = job.job_id
        context["ti"].xcom_push(key="job_id", value=self.job_id)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryInsertJobTrigger(
                conn_id=self.gcp_conn_id,
                job_id=self.job_id,
                project_id=self.project_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )


class BigQueryCheckOperatorAsync(BigQueryCheckOperator):
    """
    BigQueryCheckOperatorAsync is asynchronous operator, submit the job and check
    for the status in async mode by using the job id
    """

    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Trigger."""
        configuration = {"query": {"query": self.sql}}

        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context) -> None:  # noqa: D102
        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        job = self._submit_job(hook, job_id="")
        context["ti"].xcom_push(key="job_id", value=job.job_id)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryCheckTrigger(
                conn_id=self.gcp_conn_id,
                job_id=job.job_id,
                project_id=hook.project_id,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        records = event["records"]
        if not records:
            raise AirflowException("The query returned None")
        elif not all(bool(r) for r in records):
            raise AirflowException(f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}")
        self.log.info("Record: %s", event["records"])
        self.log.info("Success.")


class BigQueryGetDataOperatorAsync(BigQueryGetDataOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and returns data in a python list. The number of elements in the returned list will
    be equal to the number of rows fetched. Each element in the list will again be a list
    where element would represent the columns values for that row.

    **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``

    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'``.

    **Example**: ::

        get_data = BigQueryGetDataOperator(
            task_id='get_data_from_bq',
            dataset_id='test_dataset',
            table_id='Transaction_partitions',
            max_results=100,
            selected_fields='DATE',
            gcp_conn_id='airflow-conn-id'
        )

    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param max_results: The maximum number of records (rows) to be fetched from the table. (templated)
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def _submit_job(  # type: ignore[override]
        self,
        hook: BigQueryHook,
        job_id: str,
        configuration: Dict[str, Any],
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        return hook.insert_job(
            configuration=configuration,
            location=self.location,
            project_id=hook.project_id,
            job_id=job_id,
            nowait=True,
        )

    def generate_query(self) -> str:
        """
        Generate a select query if selected fields are given or with *
        for the given dataset and table id
        """
        query = "select "
        if self.selected_fields:
            query += self.selected_fields
        else:
            query += "*"
        query += " from " + self.dataset_id + "." + self.table_id + " limit " + str(self.max_results)
        return query

    def execute(self, context: Context) -> None:  # noqa: D102
        get_query = self.generate_query()
        configuration = {"query": {"query": get_query}}

        hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

        self.hook = hook
        job = self._submit_job(hook, job_id="", configuration=configuration)
        self.job_id = job.job_id
        context["ti"].xcom_push(key="job_id", value=self.job_id)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryGetDataTrigger(
                conn_id=self.gcp_conn_id,
                job_id=self.job_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                project_id=hook.project_id,
                delegate_to=self.delegate_to,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("Total extracted rows: %s", len(event["records"]))
        return event["records"]


class BigQueryIntervalCheckOperatorAsync(BigQueryIntervalCheckOperator):
    """
    Checks asynchronously that the values of metrics given as SQL expressions are within
    a certain tolerance of the ones from days_back before.

    This method constructs a query like so ::
        SELECT {metrics_threshold_dict_key} FROM {table}
        WHERE {date_filter_column}=<date>

    :param table: the table name
    :param days_back: number of days between ds and the ds we want to check
        against. Defaults to 7 days
    :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
        example 'COUNT(*)': 1.5 would require a 50 percent or less difference
        between the current day, and the prior days_back.
    :param use_legacy_sql: Whether to use legacy SQL (true)
        or standard SQL (false).
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param location: The geographic location of the job. See details at:
        https://cloud.google.com/bigquery/docs/locations#specifying_your_location
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param labels: a dictionary containing labels for the table, passed to BigQuery
    """

    def _submit_job(
        self,
        hook: BigQueryHook,
        sql: str,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        configuration = {"query": {"query": sql}}
        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context) -> None:
        """Execute the job in sync mode and defers the trigger with job id to poll for the status"""
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
        self.log.info("Using ratio formula: %s", self.ratio_formula)

        self.log.info("Executing SQL check: %s", self.sql1)
        job_1 = self._submit_job(hook, sql=self.sql1, job_id="")
        context["ti"].xcom_push(key="job_id", value=job_1.job_id)

        self.log.info("Executing SQL check: %s", self.sql2)
        job_2 = self._submit_job(hook, sql=self.sql2, job_id="")
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryIntervalCheckTrigger(
                conn_id=self.gcp_conn_id,
                first_job_id=job_1.job_id,
                second_job_id=job_2.job_id,
                project_id=hook.project_id,
                table=self.table,
                metrics_thresholds=self.metrics_thresholds,
                date_filter_column=self.date_filter_column,
                days_back=self.days_back,
                ratio_formula=self.ratio_formula,
                ignore_zero=self.ignore_zero,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["status"],
        )


class BigQueryValueCheckOperatorAsync(BigQueryValueCheckOperator):  # noqa: D101
    def _submit_job(
        self,
        hook: BigQueryHook,
        job_id: str,
    ) -> BigQueryJob:
        """Submit a new job and get the job id for polling the status using Triggerer."""
        configuration = {
            "query": {
                "query": self.sql,
                "useLegacySql": False,
            }
        }
        if self.use_legacy_sql:
            configuration["query"]["useLegacySql"] = self.use_legacy_sql

        return hook.insert_job(
            configuration=configuration,
            project_id=hook.project_id,
            location=self.location,
            job_id=job_id,
            nowait=True,
        )

    def execute(self, context: Context) -> None:  # noqa: D102
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)

        job = self._submit_job(hook, job_id="")
        context["ti"].xcom_push(key="job_id", value=job.job_id)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryValueCheckTrigger(
                conn_id=self.gcp_conn_id,
                job_id=job.job_id,
                project_id=hook.project_id,
                sql=self.sql,
                pass_value=self.pass_value,
                tolerance=self.tol,
                impersonation_chain=self.impersonation_chain,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(
            "%s completed with response %s ",
            self.task_id,
            event["message"],
        )
