"""This module contains Google GKE operators."""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator


class GKEStartPodOperatorAsync(GKEStartPodOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`
    and set `deferrable` param to `True` instead.
    """

    is_deprecated = True
    post_deprecation_replacement = (
        "from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator"
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated."
                "Please use `airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )

        super().__init__(*args, deferrable=True, **kwargs)
