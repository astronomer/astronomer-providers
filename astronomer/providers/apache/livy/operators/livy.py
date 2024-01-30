"""This module contains the Apache Livy operator async."""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.apache.livy.operators.livy import LivyOperator


class LivyOperatorAsync(LivyOperator):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.apache.livy.operators.livy.LivyOperator` instead
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            "This class is deprecated. "
            "Use `airflow.providers.apache.livy.operators.livy.LivyOperator` "
            "and set `deferrable` param to `True` instead.",
        )
        super().__init__(*args, deferrable=True, **kwargs)
