from typing import Optional, Sequence, Union

from airflow.exceptions import AirflowException
from astronomer.providers.microsoft.cloud.triggers.wasb import WasbBlobTrigger
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor


class WasbBlobSensorAsync(WasbBlobSensor):
    """
    add param definitions here
    """

    ui_color = "#f0eee4"

    def __init__(
        self,
        polling_interval: float = 5.0,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.polling_interval = polling_interval

    def execute(self, context):
        self.defer(
            timeout=self.execution_timeout,
            trigger=WasbBlobTrigger(
                container_name=self.container_name,
                blob_name=self.blob_name,
                polling_period_seconds=self.polling_interval,
                wasb_conn_id=self.wasb_conn_id
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            self.log.info(event["message"])
            raise AirflowException(event["message"])
        self.log.info("File %s was found in container %s.", self.blob_name, self.container_name)
        return event["message"]