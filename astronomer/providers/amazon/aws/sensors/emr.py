from datetime import timedelta
from typing import Any, Dict

from airflow import AirflowException
from airflow.providers.amazon.aws.sensors.emr import (
    EmrContainerSensor,
    EmrJobFlowSensor,
    EmrStepSensor,
)

from astronomer.providers.amazon.aws.triggers.emr import (
    EmrContainerSensorTrigger,
    EmrJobFlowSensorTrigger,
    EmrStepSensorTrigger,
)
from astronomer.providers.utils.typing_compat import Context


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

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=EmrContainerSensorTrigger(
                virtual_cluster_id=self.virtual_cluster_id,
                job_id=self.job_id,
                max_tries=self.max_retries,
                aws_conn_id=self.aws_conn_id,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, str]) -> None:
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


class EmrStepSensorAsync(EmrStepSensor):
    """
    Async (deferring) version of EmrStepSensor

    Asks for the state of the step until it reaches any of the target states.
    If the sensor errors out, then the task will fail
    With the default target states, sensor waits step to be COMPLETED.

    For more details see
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id: step to check the state of
    :param target_states: the target states, sensor waits until
        step reaches any of these states
    :param failed_states: the failure states, sensor fails when
        step reaches any of these states
    """

    def execute(self, context: Context) -> None:
        """Deferred and give control to trigger"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=EmrStepSensorTrigger(
                job_flow_id=self.job_flow_id,
                step_id=self.step_id,
                target_states=self.target_states,
                failed_states=self.failed_states,
                aws_conn_id=self.aws_conn_id,
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
        if event:
            if event["status"] == "error":
                raise AirflowException(event["message"])

            self.log.info(event.get("message"))
            self.log.info("%s completed successfully.", self.job_flow_id)


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

    def execute(self, context: Context) -> None:
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=EmrJobFlowSensorTrigger(
                job_flow_id=self.job_flow_id,
                aws_conn_id=self.aws_conn_id,
                target_states=self.target_states,
                failed_states=self.failed_states,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, str]) -> None:
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
