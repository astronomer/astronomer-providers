"""
A Deferrable Airflow operator for AWS Batch services

.. seealso::

    - `Configuration <http://boto3.readthedocs.io/en/latest/guide/configuration.html>`_
    - `Batch <http://boto3.readthedocs.io/en/latest/reference/services/batch.html>`_
    - `Welcome <https://docs.aws.amazon.com/batch/latest/APIReference/Welcome.html>`_
"""
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.batch import BatchOperator

from astronomer.providers.amazon.aws.triggers.batch import BatchOperatorTrigger
from astronomer.providers.utils.typing_compat import Context


class BatchOperatorAsync(BatchOperator):
    """
    Execute a job asynchronously on AWS Batch

    .. see also::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BatchOperator`

    :param job_name: the name for the job that will run on AWS Batch (templated)
    :param job_definition: the job definition name on AWS Batch
    :param job_queue: the queue name on AWS Batch
    :param overrides: the `containerOverrides` parameter for boto3 (templated)
    :param array_properties: the `arrayProperties` parameter for boto3
    :param parameters: the `parameters` for boto3 (templated)
    :param job_id: the job ID, usually unknown (None) until the
        submit_job operation gets the jobId defined by AWS Batch
    :param waiters: an :class:`.BatchWaiters` object (see note below);
        if None, polling is used with max_retries and status_retries.
    :param max_retries: exponential back-off retries, 4200 = 48 hours;
        polling is only used when waiters is None
    :param status_retries: number of HTTP retries to get job status, 10;
        polling is only used when waiters is None
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used.
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :param tags: collection of tags to apply to the AWS Batch job submission
        if None, no tags are submitted

    .. note::
        Any custom waiters must return a waiter for these calls:

            | ``waiter = waiters.get_waiter("JobExists")``
            | ``waiter = waiters.get_waiter("JobRunning")``
            | ``waiter = waiters.get_waiter("JobComplete")``
    """

    def execute(self, context: Context) -> None:
        """
        Airflow runs this method on the worker and defers using the trigger.
        Submit the job and get the job_id using which we defer and poll in trigger
        """
        self.submit_job(context)
        self.defer(
            timeout=self.execution_timeout,
            trigger=BatchOperatorTrigger(
                job_id=self.job_id,
                job_name=self.job_name,
                job_definition=self.job_definition,
                job_queue=self.job_queue,
                overrides=self.overrides,
                array_properties=self.array_properties,
                parameters=self.parameters,
                waiters=self.waiters,
                tags=self.tags,
                max_retries=self.hook.max_retries,
                status_retries=self.hook.status_retries,
                aws_conn_id=self.hook.aws_conn_id,
                region_name=self.hook.region_name,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if "status" in event and event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])
