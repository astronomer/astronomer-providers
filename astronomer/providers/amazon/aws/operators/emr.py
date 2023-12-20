import warnings

from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator


class EmrContainerOperatorAsync(EmrContainerOperator):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.apache.aws.operators.emr.EmrContainerOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)
