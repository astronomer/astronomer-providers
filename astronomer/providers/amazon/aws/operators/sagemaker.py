import time
from typing import Any, Dict, Optional

from airflow import AirflowException
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)

from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerTrainingTrigger,
    SagemakerTransformTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class SageMakerTransformOperatorAsync(SageMakerTransformOperator):
    """
    SageMakerTransformOperatorAsync Starts a transform job and poll for the status asynchronously.
    A transform job uses a trained model to get inferences on a dataset and saves these results to an Amazon
    S3 location that you specify.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:``howto/operator:SageMakerTransformOperator``

    :param config: The configuration necessary to start a transform job (templated).

        If you need to create a SageMaker transform job based on an existed SageMaker model::

            config = transform_config

        If you need to create both SageMaker model and SageMaker Transform job::

            config = {
                'Model': model_config,
                'Transform': transform_config
            }

        For details of the configuration parameter of transform_config see
        :ref:``SageMaker.Client.create_transform_job``

        For details of the configuration parameter of model_config, See:
        :ref:``SageMaker.Client.create_model``

    :param aws_conn_id: The AWS connection ID to use.
    :param check_interval: If wait is set to True, the time interval, in seconds,
        that this operation waits to check the status of the transform job.
    :param max_ingestion_time: The operation fails if the transform job doesn't finish
     within max_ingestion_time seconds. If you set this parameter to None, the operation does not timeout.
    :param check_if_job_exists: If set to true, then the operator will check whether a transform job
        already exists for the name in the config.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "increment"
        (default) and "fail".
        This is only relevant if check_if_job_exists is True.
    """

    def execute(self, context: Context) -> None:  # type: ignore[override]
        """
        Creates transform job via sync hook `create_transform_job` and pass the
        control to trigger and polls for the statis of the transform job in async
        """
        self.preprocess_config()
        model_config = self.config.get("Model")
        transform_config = self.config.get("Transform", self.config)
        if self.check_if_job_exists:
            self._check_if_transform_job_exists()
        if model_config:
            self.log.info("Creating SageMaker Model %s for transform job", model_config["ModelName"])
            self.hook.create_model(model_config)
        self.log.info("Creating SageMaker transform Job %s.", transform_config["TransformJobName"])
        response = self.hook.create_transform_job(
            transform_config,
            wait_for_completion=False,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker transform Job creation failed: {response}")
        else:
            end_time: Optional[float] = None
            if self.max_ingestion_time is not None:
                end_time = time.time() + self.max_ingestion_time
            self.defer(
                timeout=self.execution_timeout,
                trigger=SagemakerTransformTrigger(
                    poll_interval=self.check_interval,
                    end_time=end_time,
                    aws_conn_id=self.aws_conn_id,
                    job_name=self.config["TransformJobName"],
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: "Context", event: Dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])


class SageMakerTrainingOperatorAsync(SageMakerTrainingOperator):
    """
    SageMakerTrainingOperatorAsync Starts a model training job and poll for the status asynchronously.
    After training completes, Amazon SageMaker saves the resulting model artifacts to an Amazon S3 location
    that you specify.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:``howto/operator:SageMakerTrainingOperator``

    :param config: The configuration necessary to start a training job (templated).
        For details of the configuration parameter see ``SageMaker.Client.create_training_job``
    :param aws_conn_id: The AWS connection ID to use.
    :param print_log: if the operator should print the cloudwatch log during training
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the training job
    :param max_ingestion_time: The operation fails if the training job
        doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :param check_if_job_exists: If set to true, then the operator will check whether a training job
        already exists for the name in the config.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "increment"
        (default) and "fail".
        This is only relevant if check_if_job_exists is True.
    """

    def execute(self, context: Context) -> None:  # type: ignore[override]
        """
        Creates SageMaker training job via sync hook `create_training_job` and pass the
        control to trigger and polls for the statis of the transform job in async
        """
        self.preprocess_config()
        if self.check_if_job_exists:
            self._check_if_job_exists()
        self.log.info("Creating SageMaker training job %s.", self.config["TrainingJobName"])
        response = self.hook.create_training_job(
            self.config,
            wait_for_completion=self.wait_for_completion,
            print_log=self.print_log,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Training Job creation failed: {response}")
        else:
            end_time: Optional[float] = None
            if self.max_ingestion_time is not None:
                end_time = time.time() + self.max_ingestion_time
            self.defer(
                timeout=self.execution_timeout,
                trigger=SagemakerTrainingTrigger(
                    poll_interval=self.check_interval,
                    end_time=end_time,
                    aws_conn_id=self.aws_conn_id,
                    job_name=self.config["TrainingJobName"],
                ),
                method_name="execute_complete",
            )
