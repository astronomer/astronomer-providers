from __future__ import annotations

import json
import warnings
from typing import Any

from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)
from airflow.utils.json import AirflowJsonEncoder


def serialize(result: dict[str, Any]) -> str:
    """Serialize any objects coming from Sagemaker API response to json string"""
    return json.loads(json.dumps(result, cls=AirflowJsonEncoder))  # type: ignore[no-any-return]


class SageMakerProcessingOperatorAsync(SageMakerProcessingOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)


class SageMakerTransformOperatorAsync(SageMakerTransformOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)


class SageMakerTrainingOperatorAsync(SageMakerTrainingOperator):
    """
    This class is deprecated.
    Please use :class: `~airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator`
    and set `deferrable` param to `True` instead.
    """

    def __init__(self, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This module is deprecated."
                "Please use `airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator`"
                "and set `deferrable` param to `True` instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(deferrable=True, **kwargs)
