"""
This module contains operators to interact with Amazon SageMaker in an asynchronous way.

Starting release ``1.17.0`` of the ``astronomer-providers`` package, the operators in this module extend the
corresponding operators in the Amazon provider of Apache Airflow supporting the deferrable capabilities. With this,
the aim is to push support and fixes to the upstream operators as much as possible, and also we do not have to keep
track of the  upstream changes and backport them to the operators in this module.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)


class SageMakerProcessingOperatorAsync(SageMakerProcessingOperator):
    """The operator is used to start a SageMaker processing job asynchronously."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize a new SageMakerProcessingOperatorAsync instance.

        We set the deferrable flag to True while calling the super class constructor to enable the deferrable
        capabilities.

        :param args: List of arguments to pass to SageMakerProcessingOperator.
        :param kwargs: Keyword arguments to pass to SageMakerProcessingOperator.
        """
        super().__init__(*args, deferrable=True, **kwargs)


class SageMakerTransformOperatorAsync(SageMakerTransformOperator):
    """The operator is used to start a SageMaker transform job asynchronously."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize a new SageMakerTransformOperatorAsync instance.

        We set the deferrable flag to True while calling the super class constructor to enable the deferrable
        capabilities.

        :param args: List of arguments to pass to SageMakerTransformOperator.
        :param kwargs: Keyword arguments to pass to SageMakerTransformOperator.
        """
        super().__init__(*args, deferrable=True, **kwargs)


class SageMakerTrainingOperatorAsync(SageMakerTrainingOperator):
    """The operator is used to start a SageMaker training job asynchronously."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize a new SageMakerTrainingOperatorAsync instance.

        We set the deferrable flag to True while calling the super class constructor to enable the deferrable
        capabilities.

        :param args: List of arguments to pass to SageMakerTrainingOperator.
        :param kwargs: Keyword arguments to pass to SageMakerTrainingOperator.
        """
        super().__init__(*args, deferrable=True, **kwargs)
