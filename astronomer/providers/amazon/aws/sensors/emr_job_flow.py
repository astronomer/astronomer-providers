from typing import Any, Dict

from airflow import AirflowException
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

from astronomer.providers.amazon.aws.triggers.emr_job_flow import (
    EmrJobFlowSensorTrigger,
)


class EmrJobFlowSensorAsync(EmrJobFlowSensor):
    """
    Async EMR Job flow sensor polls for the cluster state until it reaches
    any of the target states.
    If it fails the sensor errors, failing the task.

    With the default target states, sensor waits cluster to be terminated.
    When target_states is set to ['RUNNING', 'WAITING'] sensor waits
    until job flow to be ready (after 'STARTING' and 'BOOTSTRAPPING' states)

    :param job_flow_id: job_flow_id to check the state of cluster
    :param target_states: the target states, sensor waits until
        job flow reaches any of these states
    :param failed_states: the failure states, sensor fails when
        job flow reaches any of these states
    """

    def __init__(
        self,
        *,
        poll_interval: float = 5,
        **kwargs: Any,
    ):
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: Dict[Any, Any]) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=self.execution_timeout,
            trigger=EmrJobFlowSensorTrigger(
                job_flow_id=self.job_flow_id,
                aws_conn_id=self.aws_conn_id,
                target_states=self.target_states,
                failed_states=self.failed_states,
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
