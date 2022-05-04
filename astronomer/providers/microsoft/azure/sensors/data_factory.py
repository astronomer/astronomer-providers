from typing import Any, Dict

from airflow import AirflowException
from airflow.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)
from airflow.utils.context import Context

from astronomer.providers.microsoft.azure.triggers.data_factory import (
    ADFPipelineRunStatusSensorTrigger,
)


class AzureDataFactoryPipelineRunStatusSensorAsync(AzureDataFactoryPipelineRunStatusSensor):
    """
    Checks the status of a pipeline run.

    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param run_id: The pipeline run identifier.
    :param resource_group_name: The resource group name.
    :param factory_name: The data factory name.
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
            timeout=self.execution_timeout,
            trigger=ADFPipelineRunStatusSensorTrigger(
                run_id=self.run_id,
                azure_data_factory_conn_id=self.azure_data_factory_conn_id,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
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
