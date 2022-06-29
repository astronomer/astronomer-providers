from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.batch import BatchSensor
from airflow.utils.context import Context

from astronomer.providers.amazon.aws.triggers.batch import BatchSensorTrigger


class BatchSensorAsync(BatchSensor):
    """
    Given a JOB ID of the Batch Job, executes asynchronously until it
    reaches a failure state or success state.
    If the job fails, the task will fail.

    .. see also::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchSensor`

    :param job_id: Batch job_id to check the state for
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :param region_name: region name to use in AWS Hook.
        Override the region_name in connection (if provided)
    :param poll_interval:  polling period in seconds to check for the status of the job.
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5,
        **kwargs: Any,
    ):
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: "Context") -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=self.execution_timeout,
            trigger=BatchSensorTrigger(
                job_id=self.job_id,
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[str, Any], event: Dict[str, Any]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if "status" in event and event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])
