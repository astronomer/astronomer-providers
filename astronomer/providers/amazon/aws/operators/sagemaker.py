from __future__ import annotations

import json
import time
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import (
    LogState,
    secondary_training_status_message,
)
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerProcessingOperator,
    SageMakerTrainingOperator,
    SageMakerTransformOperator,
)
from airflow.utils.json import AirflowJsonEncoder

from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerProcessingTrigger,
    SagemakerTrainingWithLogTrigger,
    SagemakerTrigger,
)
from astronomer.providers.utils.typing_compat import Context


def serialize(result: dict[str, Any]) -> str:
    """Serialize any objects coming from Sagemaker API response to json string"""
    return json.loads(json.dumps(result, cls=AirflowJsonEncoder))  # type: ignore[no-any-return]


class SageMakerProcessingOperatorAsync(SageMakerProcessingOperator):
    """
    SageMakerProcessingOperatorAsync is used to analyze data and evaluate machine learning
    models on Amazon SageMaker. With SageMakerProcessingOperatorAsync, you can use a simplified, managed
    experience on SageMaker to run your data processing workloads, such as feature
    engineering, data validation, model evaluation, and model interpretation.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SageMakerProcessingOperator`

    :param config: The configuration necessary to start a processing job (templated).
        For details of the configuration parameter see
        :ref:``SageMaker.Client.create_processing_job``
    :param aws_conn_id: The AWS connection ID to use.
    :param wait_for_completion: Even if wait is set to False, in async we will defer and
        the operation waits to check the status of the processing job.
    :param print_log: if the operator should print the cloudwatch log during processing
    :param check_interval: if wait is set to be true, this is the time interval
        in seconds which the operator will check the status of the processing job
    :param max_ingestion_time: The operation fails if the processing job
        doesn't finish within max_ingestion_time seconds. If you set this parameter to None,
        the operation does not timeout.
    :param action_if_job_exists: Behaviour if the job name already exists. Possible options are "increment"
        (default) and "fail".
    """

    def execute(self, context: Context) -> dict[str, str] | None:  # type: ignore[override]
        """
        Creates processing job via sync hook `create_processing_job` and pass the
        control to trigger and polls for the status of the processing job in async
        """
        self.preprocess_config()
        processing_job_name = self.config["ProcessingJobName"]
        try:
            if self.hook.count_processing_jobs_by_name(processing_job_name):
                raise AirflowException(
                    f"A SageMaker processing job with name {processing_job_name} already exists."
                )
        except AttributeError:  # pragma: no cover
            # For apache-airflow-providers-amazon<8.0.0
            if self.hook.find_processing_job_by_name(processing_job_name):
                raise AirflowException(
                    f"A SageMaker processing job with name {processing_job_name} already exists."
                )
        self.log.info("Creating SageMaker processing job %s.", self.config["ProcessingJobName"])
        response = self.hook.create_processing_job(
            self.config,
            # we do not wait for completion here but we create the processing job
            # and poll for it in trigger
            wait_for_completion=False,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Processing Job creation failed: {response}")

        response = self.hook.describe_processing_job(processing_job_name)
        status = response["ProcessingJobStatus"]
        if status in self.hook.failed_states:
            raise AirflowException(f"SageMaker job failed because {response['FailureReason']}")
        elif status == "Completed":
            self.log.info(f"{self.task_id} completed successfully.")
            return {"Processing": serialize(response)}

        end_time: float | None = None
        if self.max_ingestion_time is not None:
            end_time = time.time() + self.max_ingestion_time
        self.defer(
            timeout=self.execution_timeout,
            trigger=SagemakerProcessingTrigger(
                poll_interval=self.check_interval,
                aws_conn_id=self.aws_conn_id,
                job_name=self.config["ProcessingJobName"],
                end_time=end_time,
            ),
            method_name="execute_complete",
        )

        # for bypassing mypy missing return error
        return None  # pragma: no cover

    def execute_complete(self, context: Context, event: Any = None) -> dict[str, Any]:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and event["status"] == "success":
            self.log.info("%s completed successfully.", self.task_id)
            return {"Processing": serialize(event["message"])}
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")


class SageMakerTransformOperatorAsync(SageMakerTransformOperator):
    """
    SageMakerTransformOperatorAsync starts a transform job and polls for the status asynchronously.
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

    def execute(self, context: Context) -> dict[str, Any] | None:  # type: ignore[override]
        """
        Creates transform job via sync hook `create_transform_job` and pass the
        control to trigger and polls for the status of the transform job in async
        """
        self.preprocess_config()
        model_config = self.config.get("Model")
        transform_config = self.config.get("Transform", self.config)
        if self.check_if_job_exists:  # pragma: no cover
            try:
                # for apache-airflow-providers-amazon<=7.2.1
                self._check_if_transform_job_exists()  # type: ignore[attr-defined]
            except AttributeError:
                # for apache-airflow-providers-amazon>=7.3.0
                transform_config["TransformJobName"] = self._get_unique_job_name(
                    transform_config["TransformJobName"],
                    self.action_if_job_exists == "fail",
                    self.hook.describe_transform_job,
                )
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

        response = self.hook.describe_transform_job(transform_config["TransformJobName"])
        status = response["TransformJobStatus"]
        if status in self.hook.failed_states:
            raise AirflowException(f"SageMaker job failed because {response['FailureReason']}")

        if status == "Completed":
            self.log.info(f"{self.task_id} completed successfully.")
            return {
                "Model": serialize(self.hook.describe_model(transform_config["ModelName"])),
                "Transform": serialize(response),
            }

        end_time: float | None = None
        if self.max_ingestion_time is not None:
            end_time = time.time() + self.max_ingestion_time
        self.defer(
            timeout=self.execution_timeout,
            trigger=SagemakerTrigger(
                poke_interval=self.check_interval,
                end_time=end_time,
                aws_conn_id=self.aws_conn_id,
                job_name=transform_config["TransformJobName"],
                job_type="Transform",
                response_key="TransformJobStatus",
            ),
            method_name="execute_complete",
        )

        # for bypassing mypy missing return error
        return None  # pragma: no cover

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:  # type: ignore[override]
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and event["status"] == "success":
            self.log.info("%s completed successfully.", self.task_id)
            transform_config = self.config.get("Transform", self.config)
            return {
                "Model": serialize(self.hook.describe_model(transform_config["ModelName"])),
                "Transform": serialize(event["message"]),
            }
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")


class SageMakerTrainingOperatorAsync(SageMakerTrainingOperator):
    """
    SageMakerTrainingOperatorAsync starts a model training job and polls for the status asynchronously.
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

    def execute(self, context: Context) -> dict[str, Any] | None:  # type: ignore[override]
        """
        Creates SageMaker training job via sync hook `create_training_job` and pass the
        control to trigger and polls for the status of the training job in async
        """
        self.preprocess_config()
        if self.check_if_job_exists:  # pragma: no cover
            try:
                # for apache-airflow-providers-amazon<=7.2.1
                self._check_if_job_exists()  # type: ignore[call-arg]
            except TypeError:
                # for apache-airflow-providers-amazon>=7.3.0
                self.config["TrainingJobName"] = self._get_unique_job_name(
                    self.config["TrainingJobName"],
                    self.action_if_job_exists == "fail",
                    self.hook.describe_training_job,
                )
        self.log.info("Creating SageMaker training job %s.", self.config["TrainingJobName"])
        response = self.hook.create_training_job(
            self.config,
            wait_for_completion=False,
            print_log=False,
            check_interval=self.check_interval,
            max_ingestion_time=self.max_ingestion_time,
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise AirflowException(f"Sagemaker Training Job creation failed: {response}")

        end_time: float | None = None
        if self.max_ingestion_time is not None:
            end_time = time.time() + self.max_ingestion_time

        description = self.hook.describe_training_job(self.config["TrainingJobName"])
        status = description["TrainingJobStatus"]
        if self.print_log:
            instance_count = description["ResourceConfig"]["InstanceCount"]
            last_describe_job_call = time.monotonic()
            job_already_completed = status not in self.hook.non_terminal_states
            _, last_description, last_describe_job_call = self.hook.describe_training_job_with_log(
                self.config["TrainingJobName"],
                {},
                [],
                instance_count,
                LogState.TAILING if job_already_completed else LogState.COMPLETE,
                description,
                last_describe_job_call,
            )

            self.log.info(secondary_training_status_message(description, None))

            if status in self.hook.failed_states:
                reason = last_description.get("FailureReason", "(No reason provided)")
                raise AirflowException(f"SageMaker job failed because {reason}")
            elif status == "Completed":
                billable_time = (
                    last_description["TrainingEndTime"] - last_description["TrainingStartTime"]
                ) * instance_count
                self.log.info(
                    f"Billable seconds: {int(billable_time.total_seconds()) + 1}\n"
                    f"{self.task_id} completed successfully."
                )
                return {"Training": serialize(description)}

            self.defer(
                timeout=self.execution_timeout,
                trigger=SagemakerTrainingWithLogTrigger(
                    poke_interval=self.check_interval,
                    end_time=end_time,
                    aws_conn_id=self.aws_conn_id,
                    job_name=self.config["TrainingJobName"],
                    instance_count=int(instance_count),
                    status=status,
                ),
                method_name="execute_complete",
            )
        else:
            if status in self.hook.failed_states:
                raise AirflowException(f"SageMaker job failed because {description['FailureReason']}")
            elif status == "Completed":
                self.log.info(f"{self.task_id} completed successfully.")
                return {"Training": serialize(description)}

            self.defer(
                timeout=self.execution_timeout,
                trigger=SagemakerTrigger(
                    poke_interval=self.check_interval,
                    end_time=end_time,
                    aws_conn_id=self.aws_conn_id,
                    job_name=self.config["TrainingJobName"],
                    job_type="Training",
                    response_key="TrainingJobStatus",
                ),
                method_name="execute_complete",
            )

        # for bypassing mypy missing return error
        return None  # pragma: no cover

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:  # type: ignore[override]
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and event["status"] == "success":
            self.log.info("%s completed successfully.", self.task_id)
            return {"Training": serialize(event["message"])}
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
