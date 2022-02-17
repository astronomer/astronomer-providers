import warnings

from astronomer_operators.core.sensors.external_task import ExternalTaskSensorAsync
from astronomer_operators.core.triggers.external_task import (
    DagStateTrigger,
    TaskStateTrigger,
)

DEPRECATED_NAMES = {
    "ExternalTaskSensorAsync": "astronomer_operators.core.sensors.external_task",
    "DagStateTrigger": "astronomer_operators.core.triggers.external_task",
    "TaskStateTrigger": "astronomer_operators.core.triggers.external_task",
}


def __getattr__(name):
    if name in DEPRECATED_NAMES:
        mod = DEPRECATED_NAMES[name]
        warnings.warn(
            f"Importing {name} from `astronomer_operators.external_task` is deprecated; please use `{mod}`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]


__all__ = ["DagStateTrigger", "ExternalTaskSensorAsync", "TaskStateTrigger"]
