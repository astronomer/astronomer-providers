from __future__ import annotations

import warnings
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)


class PodNotFoundException(AirflowException):
    """Expected pod does not exist in kube-api."""


class KubernetesPodOperatorAsync(KubernetesPodOperator):
    """
    This class is deprecated.

    Please use :class: `~airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, **kwargs: Any):
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
