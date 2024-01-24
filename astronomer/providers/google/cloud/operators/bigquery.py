"""This module contains Google BigQueryAsync providers."""
from __future__ import annotations

import warnings
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook, BigQueryJob
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)

from astronomer.providers.google.cloud.triggers.bigquery import (
    BigQueryIntervalCheckTrigger,
    BigQueryValueCheckTrigger,
)
from astronomer.providers.utils.typing_compat import Context

BIGQUERY_JOB_DETAILS_LINK_FMT = "https://console.cloud.google.com/bigquery?j={job_id}"


class BigQueryInsertJobOperatorAsync(BigQueryInsertJobOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )

        poll_interval: float = kwargs.pop("poll_interval", 4.0)
        super().__init__(*args, deferrable=True, **kwargs)
        self.poll_interval = poll_interval


class BigQueryCheckOperatorAsync(BigQueryCheckOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        poll_interval: float = kwargs.pop("poll_interval", 4.0)
        super().__init__(*args, deferrable=True, **kwargs)
        self.poll_interval = poll_interval


class BigQueryGetDataOperatorAsync(BigQueryGetDataOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        poll_interval: float = kwargs.pop("poll_interval", 4.0)
        super().__init__(*args, deferrable=True, **kwargs)
        self.poll_interval = poll_interval


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
    :param poll_interval: polling period in seconds to check for the status of job. Defaults to 4 seconds.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        poll_interval: float = kwargs.pop("poll_interval", 4.0)
        super().__init__(*args, **kwargs)
        self.poll_interval = poll_interval

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
        if job_1.running() or job_2.running():
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
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context=context)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
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
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        poll_interval: float = kwargs.pop("poll_interval", 4.0)
        super().__init__(*args, **kwargs)
        self.poll_interval = poll_interval

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
        if job.running():
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
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context=context)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
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
