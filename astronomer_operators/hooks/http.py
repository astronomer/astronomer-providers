import warnings

from astronomer_operators.http.hooks.http import HttpHookAsync

warnings.warn(
    "This module is deprecated. Please use `astronomer_operators.http.hooks.http`.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["HttpHookAsync"]
