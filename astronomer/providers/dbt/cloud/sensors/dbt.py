import time
from typing import Any, Dict, Optional

from airflow import AirflowException
from airflow.providers.dbt.cloud.sensors.dbt import DbtCloudJobRunSensor
from airflow.utils.context import Context

from astronomer.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger


class DbtCloudJobRunSensorAsync(DbtCloudJobRunSensor):
    """
    Checks the status of a dbt Cloud job run.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:DbtCloudJobRunSensor`

    :param dbt_cloud_conn_id: The connection identifier for connecting to dbt Cloud.
    :param run_id: The job run identifier.
    :param account_id: The dbt Cloud account identifier.
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
        end_time = time.time() + self.timeout
        self.defer(
            timeout=self.execution_timeout,
            trigger=DbtCloudRunJobTrigger(
                run_id=self.run_id,
                wait_for_termination=False,
                conn_id=self.dbt_cloud_conn_id,
                account_id=self.account_id,
                poll_interval=self.poll_interval,
                end_time=end_time,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[Any, Any], event: Dict[str, Any]) -> Optional[int]:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and "status" in event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            elif event["status"] == "cancelled":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
            run_id: int = event["run_id"]
            return run_id
        return None
