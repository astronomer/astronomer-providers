import warnings

from astronomer.operators.http.operators.http import HttpSensorAsync
from astronomer.operators.http.triggers.http import HttpTrigger

DEPRECATED_NAMES = {
    "HttpSensorAsync": "astronomer.operators.http.operators.http",
    "HttpTrigger": "astronomer.operators.http.triggers.http",
}


def __getattr__(name):
    if name in DEPRECATED_NAMES:
        mod = DEPRECATED_NAMES[name]
        warnings.warn(
            f"Importing {name} from `astronomer.operators.http` is deprecated; please use `{mod}`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]


__all__ = ["HttpSensorAsync", "HttpTrigger"]
