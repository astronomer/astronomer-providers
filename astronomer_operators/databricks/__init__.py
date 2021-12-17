import warnings

from astronomer_operators.databricks.operators.databricks import (
    DatabricksRunNowOperatorAsync,
    DatabricksSubmitRunOperatorAsync,
)
from astronomer_operators.databricks.triggers.databricks import DatabricksTrigger

DEPRECATED_NAMES = {
    "DatabricksTrigger": "astronomer_operators.databricks.triggers.databricks",
    "DatabricksSubmitRunOperatorAsync": "astronomer_operators.databricks.operators.databricks",
    "DatabricksRunNowOperatorAsync": "astronomer_operators.databricks.operators.databricks",
}


def __getattr__(name):
    if name in DEPRECATED_NAMES:
        mod = DEPRECATED_NAMES[name]
        warnings.warn(
            f"Importing {name} from `astronomer_operators.databricks` is deprecated; please use `{mod}`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]


__all__ = ["DatabricksRunNowOperatorAsync", "DatabricksSubmitRunOperatorAsync", "DatabricksTrigger"]
