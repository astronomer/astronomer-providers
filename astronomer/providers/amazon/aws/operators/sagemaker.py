from __future__ import annotations

from typing import Any

from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)


class SageMakerProcessingOperatorAsync(SageMakerProcessingOperator):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, deferrable=True, **kwargs)


class SageMakerTransformOperatorAsync(SageMakerTransformOperator):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, deferrable=True, **kwargs)


class SageMakerTrainingOperatorAsync(SageMakerTrainingOperator):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, deferrable=True, **kwargs)
