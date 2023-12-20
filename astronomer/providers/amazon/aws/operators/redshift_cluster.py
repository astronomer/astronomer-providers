import warnings

from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)


class RedshiftDeleteClusterOperatorAsync(RedshiftDeleteClusterOperator):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.apache.aws.operators.redshift_cluster.RedshiftDeleteClusterOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)


class RedshiftResumeClusterOperatorAsync(RedshiftResumeClusterOperator):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.apache.aws.operators.redshift_cluster.RedshiftResumeClusterOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)


class RedshiftPauseClusterOperatorAsync(RedshiftPauseClusterOperator):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.apache.aws.operators.redshift_cluster.RedshiftPauseClusterOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)
