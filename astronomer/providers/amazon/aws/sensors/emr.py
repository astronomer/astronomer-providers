from __future__ import annotations

import warnings
from datetime import timedelta
from typing import Any

from airflow import AirflowException
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.sensors.emr import (
    EmrContainerSensor,
    EmrJobFlowSensor,
    EmrStepSensor,
)

from astronomer.providers.amazon.aws.triggers.emr import (
    EmrJobFlowSensorTrigger,
)
from astronomer.providers.utils.sensor_util import raise_error_or_skip_exception
from astronomer.providers.utils.typing_compat import Context


class EmrContainerSensorAsync(EmrContainerSensor):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.sensors.emr.EmrContainerSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)


class EmrStepSensorAsync(EmrStepSensor):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            (
                "This module is deprecated. "
                "Please use `airflow.providers.amazon.aws.sensors.emr.EmrStepSensor` "
                "and set deferrable to True instead."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        return super().__init__(*args, deferrable=True, **kwargs)


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
        emr_client = self.hook.conn
        self.log.info("Poking cluster %s", self.job_flow_id)
        response = emr_client.describe_cluster(ClusterId=self.job_flow_id)
        state = response["Cluster"]["Status"]["State"]
        self.log.info("Job flow currently %s", state)

        if state == "TERMINATED":
            return None

        if state == "TERMINATED_WITH_ERRORS":
            if self.soft_fail:  # pragma: no cover
                AirflowSkipException(f"EMR job failed: {self.failure_message_from_response(response)}")
            raise AirflowException(f"EMR job failed: {self.failure_message_from_response(response)}")

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

    def execute_complete(self, context: Context, event: dict[str, str]) -> None:  # type: ignore[override]
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "error":
                raise_error_or_skip_exception(self.soft_fail, event["message"])
            self.log.info(event["message"])
        return None
