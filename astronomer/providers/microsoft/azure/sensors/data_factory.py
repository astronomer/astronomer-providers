import warnings
from datetime import timedelta
from typing import Any, Dict

from airflow import AirflowException
from airflow.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)

from astronomer.providers.microsoft.azure.triggers.data_factory import (
    ADFPipelineRunStatusSensorTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class AzureDataFactoryPipelineRunStatusSensorAsync(AzureDataFactoryPipelineRunStatusSensor):
    """
    Checks the status of a pipeline run.

    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param run_id: The pipeline run identifier.
    :param resource_group_name: The resource group name.
    :param factory_name: The data factory name.
    :param poll_interval: polling period in seconds to check for the status
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
        """Defers trigger class to poll for state of the job run until it reaches a failure state or success state"""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=ADFPipelineRunStatusSensorTrigger(
                run_id=self.run_id,
                azure_data_factory_conn_id=self.azure_data_factory_conn_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
                poke_interval=self.poke_interval,
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
