import time
from typing import Any, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerProcessingOperator

from astronomer.providers.amazon.aws.triggers.sagemaker import (
    SagemakerProcessingTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class SageMakerProcessingOperatorAsync(SageMakerProcessingOperator):
    """
    SageMakerProcessingOperatorAsync is used to analyze data and evaluate machine learning
    models on Amazon SageMake. With Processing, you can use a simplified, managed
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

    def execute(self, context: Context) -> None:  # type: ignore[override]
        """
        Creates processing job via sync hook `create_processing_job` and pass the
        control to trigger and polls for the status of the processing job in async
        """
        self.preprocess_config()
        processing_job_name = self.config["ProcessingJobName"]
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
        else:
            end_time: Optional[float] = None
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

    def execute_complete(self, context: Context, event: Any = None) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and event["status"] == "success":
            self.log.info("%s completed successfully.", self.task_id)
            return event["message"]
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        raise AirflowException("No event received in trigger callback")
