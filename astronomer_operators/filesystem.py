import warnings

from astronomer_operators.core.sensors.filesystem import FileSensorAsync
from astronomer_operators.core.triggers.filesystem import FileTrigger

DEPRECATED_NAMES = {
    "FileSensorAsync": "astronomer_operators.core.sensors.filesystem",
    "FileTrigger": "astronomer_operators.core.triggers.filesystem",
}


def __getattr__(name):
    if name in DEPRECATED_NAMES:
        mod = DEPRECATED_NAMES[name]
        warnings.warn(
            f"Importing {name} from `astronomer_operators.filesystem` is deprecated; please use `{mod}`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]


__all__ = ["FileSensorAsync", "FileTrigger"]
