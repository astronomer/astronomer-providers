from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException, AirflowSkipException

if TYPE_CHECKING:  # pragma: no cover
    from astronomer.providers.utils.typing_compat import Context


def poke(cls: Any, context: Context) -> bool | None:  # pragma: no cover
    """Wrapper to call the sensor method"""
    try:
        return cls.poke(context)  # type: ignore[no-any-return]
    except Exception as e:
        if cls.soft_fail:
            raise AirflowSkipException(str(e))
        else:
            raise e


def handle_error(soft_fail: bool, error_message: str) -> None:  # pragma: no cover
    """Raise error based on soft_fail flag"""
    if soft_fail:
        raise AirflowSkipException(error_message)
    raise AirflowException(error_message)
