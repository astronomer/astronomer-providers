import warnings

from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator


class EmrContainerOperatorAsync(EmrContainerOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.emr.EmrContainerOperator`.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.operators.emr.EmrContainerOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)
