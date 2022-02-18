import warnings

from astronomer.providers.http.operators.http import HttpSensorAsync
from astronomer.providers.http.triggers.http import HttpTrigger

DEPRECATED_NAMES = {
    "HttpSensorAsync": "astronomer.providers.http.providers.http",
    "HttpTrigger": "astronomer.providers.http.triggers.http",
}


def __getattr__(name):
    if name in DEPRECATED_NAMES:
        mod = DEPRECATED_NAMES[name]
        warnings.warn(
            f"Importing {name} from `astronomer.providers.http` is deprecated; please use `{mod}`.",
            DeprecationWarning,
            stacklevel=2,
        )
        return globals()[name]


__all__ = ["HttpSensorAsync", "HttpTrigger"]
