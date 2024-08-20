import warnings
from typing import Any

from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


class RedshiftDataOperatorAsync(RedshiftDataOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator`
    and set `deferrable` param to `True` instead.
    """

    is_deprecated = True
    post_deprecation_replacement = (
        "from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator"
    )

    def __init__(
        self,
        **kwargs: Any,
    ) -> None:
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.amazon.aws.operators.redshift_data.RedshiftDataOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
