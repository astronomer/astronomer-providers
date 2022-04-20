import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryPipelineRunStatus,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryHookAsync,
)


class ADFPipelineRunStatusSensorTrigger(BaseTrigger):
    """
    ADFPipelineRunStatusSensorTrigger is fired as deferred class with params to run the task in trigger worker, when
    ADF Pipeline is running

    :param run_id: The pipeline run identifier.
    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param poll_interval:  polling period in seconds to check for the status
    :param resource_group_name: The resource group name.
    :param factory_name: The data factory name.
    """

    def __init__(
        self,
        run_id: str,
        azure_data_factory_conn_id: str,
        poll_interval: float,
        resource_group_name: Optional[str] = None,
        factory_name: Optional[str] = None,
    ):
        super().__init__()
        self.run_id = run_id
        self.azure_data_factory_conn_id = azure_data_factory_conn_id
        self.resource_group_name = resource_group_name
        self.factory_name = factory_name
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes ADFPipelineRunStatusSensorTrigger arguments and classpath."""
        return (
            "astronomer.providers.microsoft.azure.triggers.data_factory.ADFPipelineRunStatusSensorTrigger",
            {
                "run_id": self.run_id,
                "azure_data_factory_conn_id": self.azure_data_factory_conn_id,
                "resource_group_name": self.resource_group_name,
                "factory_name": self.factory_name,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make async connection to Azure Data Factory, polls for the pipeline run status"""
        hook = AzureDataFactoryHookAsync(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        try:
            while True:
                pipeline_status = await hook.get_adf_pipeline_run_status(
                    run_id=self.run_id,
                    resource_group_name=self.resource_group_name,
                    factory_name=self.factory_name,
                )
                if pipeline_status == AzureDataFactoryPipelineRunStatus.FAILED:
                    yield TriggerEvent(
                        {"status": "error", "message": f"Pipeline run {self.run_id} has Failed."}
                    )
                    return
                elif pipeline_status == AzureDataFactoryPipelineRunStatus.CANCELLED:
                    msg = f"Pipeline run {self.run_id} has been Cancelled."
                    yield TriggerEvent({"status": "error", "message": msg})
                    return
                elif pipeline_status == AzureDataFactoryPipelineRunStatus.SUCCEEDED:
                    msg = f"Pipeline run {self.run_id} has been Succeeded."
                    yield TriggerEvent({"status": "success", "message": msg})
                    return
                await asyncio.sleep(self.poll_interval)
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
