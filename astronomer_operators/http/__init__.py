import warnings

from astronomer_operators.http.operators.http import HttpSensorAsync
from astronomer_operators.http.triggers.http import HttpTrigger

DEPRECATED_NAMES = {
    "HttpSensorAsync": "astronomer_operators.http.operators.http",
    "HttpTrigger": "astronomer_operators.http.triggers.http",
}


def __getattr__(name):
    if name in DEPRECATED_NAMES:
        mod = DEPRECATED_NAMES[name]
        warnings.warn(
            f"Importing {name} from `astronomer_operators.http` is deprecated; please use `{mod}`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]


__all__ = ["HttpSensorAsync", "HttpTrigger"]
