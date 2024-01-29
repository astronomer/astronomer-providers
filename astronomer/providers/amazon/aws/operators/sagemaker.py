from __future__ import annotations

import json
import time
import warnings
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
    SagemakerTrainingWithLogTrigger,
    SagemakerTrigger,
)
from astronomer.providers.utils.typing_compat import Context


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
