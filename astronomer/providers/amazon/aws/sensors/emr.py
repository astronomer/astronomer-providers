from typing import Any, Dict

from airflow import AirflowException
from airflow.providers.amazon.aws.sensors.emr import EmrContainerSensor

from astronomer.providers.amazon.aws.triggers.emr import EmrContainerSensorTrigger


class EmrContainerSensorAsync(EmrContainerSensor):
    """
    EmrContainerSensorAsync is async version of EmrContainerSensor,
    Asks for the state of the job run until it reaches a failure state or success state.
    If the job run fails, the task will fail.

    :param virtual_cluster_id: Reference Emr cluster id
    :param job_id: job_id to check the state
    :param max_retries: Number of times to poll for query state before
        returning the current state, defaults to None
    :param aws_conn_id: aws connection to use, defaults to ``aws_default``
    :param poll_interval: Time in seconds to wait between two consecutive call to
        check query status on athena, defaults to 10
    """

    def execute(self, context: Dict[Any, Any]) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=self.execution_timeout,
            trigger=EmrContainerSensorTrigger(
                virtual_cluster_id=self.virtual_cluster_id,
                job_id=self.job_id,
                max_retries=self.max_retries,
                aws_conn_id=self.aws_conn_id,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[Any, Any], event: Dict[str, str]) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
        return None
