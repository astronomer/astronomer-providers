"""Airflow classes that moved to the Task SDK in Airflow 3, with their Airflow 2 location as fallback.

The Airflow 2 paths still work on Airflow 3 but raise ``DeprecatedImportWarning``. Each name
has its own fallback because the Task SDK gained them in different releases.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # mypy runs against Airflow 2, where ``airflow.sdk`` is missing and would type all of these as Any.
    from airflow.exceptions import AirflowSkipException
    from airflow.hooks.base import BaseHook
    from airflow.models.param import ParamsDict
    from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
    from airflow.utils.context import Context
else:
    try:
        from airflow.sdk import BaseSensorOperator, Context, PokeReturnValue
    except ImportError:  # Airflow 2
        from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
        from airflow.utils.context import Context

    try:
        from airflow.sdk.definitions.param import ParamsDict
    except ImportError:  # Airflow 2
        from airflow.models.param import ParamsDict

    try:
        from airflow.sdk import BaseHook
    except ImportError:  # Airflow < 3.1
        from airflow.hooks.base import BaseHook

    try:
        from airflow.sdk.exceptions import AirflowSkipException
    except ImportError:  # Airflow < 3.2
        from airflow.exceptions import AirflowSkipException

__all__ = [
    "AirflowSkipException",
    "BaseHook",
    "BaseSensorOperator",
    "Context",
    "ParamsDict",
    "PokeReturnValue",
]
