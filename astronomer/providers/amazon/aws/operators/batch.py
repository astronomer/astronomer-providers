"""
A Deferrable Airflow operator for AWS Batch services

.. seealso::

    - `Configuration <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`_
    - `Batch <http://boto3.readthedocs.io/en/latest/reference/services/batch.html>`_
    - `Welcome <https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html>`_
"""

from __future__ import annotations

import warnings
from typing import Any

from airflow.providers.amazon.aws.operators.batch import BatchOperator


class BatchOperatorAsync(BatchOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.batch.BatchOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.amazon.aws.operators.batch.BatchOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
