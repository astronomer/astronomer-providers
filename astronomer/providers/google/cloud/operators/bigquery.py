"""This module contains Google BigQueryAsync providers."""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryGetDataOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryValueCheckOperator,
)

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

        # https://github.com/python/mypy/issues/6799#issuecomment-1882059741
        super().__init__(*args, deferrable=True, **kwargs)  # type: ignore[misc]


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
        super().__init__(*args, deferrable=True, **kwargs)


class BigQueryGetDataOperatorAsync(BigQueryGetDataOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, use_legacy_sql: bool = False, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)
        self.use_legacy_sql = use_legacy_sql


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
        super().__init__(*args, deferrable=True, **kwargs)


class BigQueryValueCheckOperatorAsync(BigQueryValueCheckOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)
