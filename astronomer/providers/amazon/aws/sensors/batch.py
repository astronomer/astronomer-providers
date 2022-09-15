import warnings
from datetime import timedelta
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.sensors.batch import BatchSensor

from astronomer.providers.amazon.aws.triggers.batch import BatchSensorTrigger
from astronomer.providers.utils.typing_compat import Context


class BatchSensorAsync(BatchSensor):
    """
    Given a job ID of a Batch Job, poll for the job status asynchronously until it
    reaches a failure or a success state.
    If the job fails, the task will fail.

    .. see also::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchSensor`

    :param job_id: Batch job_id to check the state for
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :param region_name: region name to use in AWS Hook
        Override the region_name in connection (if provided)
    :param poll_interval: polling period in seconds to check for the status of the job
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5,
        **kwargs: Any,
    ):
        # TODO: Remove once deprecated
        if poll_interval:
            self.poke_interval = poll_interval
            warnings.warn(
                "Argument `poll_interval` is deprecated and will be removed "
                "in a future release.  Please use  `poke_interval` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure or a success state"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=BatchSensorTrigger(
                job_id=self.job_id,
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                poke_interval=self.poke_interval,
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
