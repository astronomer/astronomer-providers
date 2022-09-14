import time
from typing import Dict

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.data_factory import AzureDataFactoryHook
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)

from astronomer.providers.microsoft.azure.triggers.data_factory import (
    AzureDataFactoryTrigger,
)
from astronomer.providers.utils.typing_compat import Context


class AzureDataFactoryRunPipelineOperatorAsync(AzureDataFactoryRunPipelineOperator):
    """
    Executes a data factory pipeline asynchronously.

    :param azure_data_factory_conn_id: The connection identifier for connecting to Azure Data Factory.
    :param pipeline_name: The name of the pipeline to execute.
    :param wait_for_termination: Flag to wait on a pipeline run's termination.  By default, this feature is
        enabled but could be disabled to perform an asynchronous wait for a long-running pipeline execution
        using the ``AzureDataFactoryPipelineRunSensor``.
    :param resource_group_name: The resource group name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the resource group name provided in the corresponding
        connection.
    :param factory_name: The data factory name. If a value is not passed in to the operator, the
        ``AzureDataFactoryHook`` will attempt to use the factory name name provided in the corresponding
        connection.
    :param reference_pipeline_run_id: The pipeline run identifier. If this run ID is specified the parameters
        of the specified run will be used to create a new run.
    :param is_recovery: Recovery mode flag. If recovery mode is set to `True`, the specified referenced
        pipeline run and the new run will be grouped under the same ``groupId``.
    :param start_activity_name: In recovery mode, the rerun will start from this activity. If not specified,
        all activities will run.
    :param start_from_failure: In recovery mode, if set to true, the rerun will start from failed activities.
        The property will be used only if ``start_activity_name`` is not specified.
    :param parameters: Parameters of the pipeline run. These parameters are referenced in a pipeline via
        ``@pipeline().parameters.parameterName`` and will be used only if the ``reference_pipeline_run_id`` is
        not specified.
    :param timeout: Time in seconds to wait for a pipeline to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True.
    :param check_interval: Time in seconds to check on a pipeline run's status for non-asynchronous waits.
        Used only if ``wait_for_termination``
    """

    def execute(self, context: Context) -> None:
        """Submits a job which generates a run_id and gets deferred"""
        hook = AzureDataFactoryHook(azure_data_factory_conn_id=self.azure_data_factory_conn_id)
        response = hook.run_pipeline(
            pipeline_name=self.pipeline_name,
            resource_group_name=self.resource_group_name,
            factory_name=self.factory_name,
            reference_pipeline_run_id=self.reference_pipeline_run_id,
            is_recovery=self.is_recovery,
            start_activity_name=self.start_activity_name,
            start_from_failure=self.start_from_failure,
            parameters=self.parameters,
        )
        run_id = vars(response)["run_id"]
        context["ti"].xcom_push(key="run_id", value=run_id)
        end_time = time.time() + self.timeout
        self.defer(
            timeout=self.execution_timeout,
            trigger=AzureDataFactoryTrigger(
                azure_data_factory_conn_id=self.azure_data_factory_conn_id,
                run_id=run_id,
                wait_for_termination=self.wait_for_termination,
                resource_group_name=self.resource_group_name,
                factory_name=self.factory_name,
                check_interval=self.check_interval,
                end_time=end_time,
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
