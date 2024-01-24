import warnings
from typing import Any

from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


class RedshiftDataOperatorAsync(RedshiftDataOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(
        self,
        *,
        poll_interval: int = 5,
        **kwargs: Any,
    ) -> None:
        warnings.warn(
            (
                "This module is deprecated and will be removed in 2.0.0."
                "Please use `airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        kwargs["poll_interval"] = poll_interval
        super().__init__(deferrable=True, **kwargs)
