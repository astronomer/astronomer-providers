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
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        poll_interval: float = kwargs.pop("poll_interval", 4.0)
        super().__init__(*args, deferrable=True, **kwargs)
        self.poll_interval = poll_interval


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
