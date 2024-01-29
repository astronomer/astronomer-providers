"""This module contains Google Dataproc operators."""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)


class DataprocCreateClusterOperatorAsync(DataprocCreateClusterOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use `airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        kwargs["polling_interval_seconds"] = polling_interval
        super().__init__(deferrable=True, **kwargs)


class DataprocDeleteClusterOperatorAsync(DataprocDeleteClusterOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use `airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        kwargs["polling_interval_seconds"] = polling_interval
        super().__init__(deferrable=True, **kwargs)
        if self.timeout is None:
            self.timeout: float = 24 * 60 * 60


class DataprocSubmitJobOperatorAsync(DataprocSubmitJobOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)


class DataprocUpdateClusterOperatorAsync(DataprocUpdateClusterOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *,
        polling_interval: float = 5.0,
        **kwargs: Any,
    ):
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use `airflow.providers.google.cloud.operators.dataproc.DataprocUpdateClusterOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        kwargs["polling_interval_seconds"] = polling_interval
        super().__init__(deferrable=True, **kwargs)
        if self.timeout is None:
            self.timeout: float = 24 * 60 * 60
