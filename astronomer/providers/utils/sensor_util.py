from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:  # pragma: no cover
    from astronomer.providers.utils.typing_compat import Context


def poke(cls: BaseSensorOperator, context: Context) -> bool:
    """Wrapper to call the sensor method"""
    try:
        return cls.poke(context)  # type: ignore[return-value]
    except Exception as e:
        if cls.soft_fail:
            raise AirflowSkipException(str(e))
        else:  # pragma: no cover
            raise e


def raise_error_or_skip_exception(soft_fail: bool, error_message: str) -> None:
    """Raise error based on soft_fail flag"""
    if soft_fail:
        raise AirflowSkipException(error_message)
    raise AirflowException(error_message)
