import warnings

from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftDeleteClusterOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)


class RedshiftDeleteClusterOperatorAsync(RedshiftDeleteClusterOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterOperator`.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftDeleteClusterOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)


class RedshiftResumeClusterOperatorAsync(RedshiftResumeClusterOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftResumeClusterOperator`.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.apache.aws.operators.redshift_cluster.RedshiftResumeClusterOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)


class RedshiftPauseClusterOperatorAsync(RedshiftPauseClusterOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftPauseClusterOperator`.
    """

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.apache.aws.operators.redshift_cluster.RedshiftPauseClusterOperator` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, deferrable=True, **kwargs)
