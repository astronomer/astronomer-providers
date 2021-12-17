import warnings

from astronomer_operators.databricks.hooks.databricks import DatabricksHookAsync

warnings.warn(
    "This module is deprecated. Please use `astronomer_operators.databricks.hooks.databricks`.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["DatabricksHookAsync"]
