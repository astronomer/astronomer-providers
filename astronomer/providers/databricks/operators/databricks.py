from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)


class DatabricksSubmitRunOperatorAsync(DatabricksSubmitRunOperator):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator` and set
    `deferrable` param to `True` instead.
    """

    is_deprecated = True
    post_deprecation_replacement = (
        "from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator"
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            "This class is deprecated."
            "Use `airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator` "
            "and set `deferrable` param to `True` instead."
        )
        super().__init__(*args, deferrable=True, **kwargs)


class DatabricksRunNowOperatorAsync(DatabricksRunNowOperator):
    """
    This class is deprecated.
    Use :class: `~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator` and set
    `deferrable` param to `True` instead.
    """

    is_deprecated = True
    post_deprecation_replacement = (
        "from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator"
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            "This class is deprecated."
            "Use `airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator` "
            "and set `deferrable` param to `True` instead."
        )
        super().__init__(*args, deferrable=True, **kwargs)
