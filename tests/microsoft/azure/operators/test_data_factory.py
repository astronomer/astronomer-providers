from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.microsoft.azure.hooks.data_factory import (
    AzureDataFactoryPipelineRunException,
    AzureDataFactoryPipelineRunStatus,
)

from astronomer.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperatorAsync,
)
from astronomer.providers.microsoft.azure.triggers.data_factory import (
    AzureDataFactoryTrigger,
)
from tests.utils.airflow_util import create_context

AZ_PIPELINE_RUN_ID = "7f8c6c72-c093-11ec-a83d-0242ac120007"

MODULE = "astronomer.providers.microsoft.azure.operators.data_factory"


class TestAzureDataFactoryRunPipelineOperatorAsync:
    OPERATOR = AzureDataFactoryRunPipelineOperatorAsync(
        task_id="run_pipeline",
        pipeline_name="pipeline",
        parameters={"myParam": "value"},
        factory_name="factory_name",
        resource_group_name="resource_group_name",
    )

    @mock.patch(f"{MODULE}.AzureDataFactoryRunPipelineOperatorAsync.defer")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_pipeline_run_status",
        return_value=AzureDataFactoryPipelineRunStatus.SUCCEEDED,
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
    def test_azure_data_factory_run_pipeline_operator_async_succeeded_before_deferred(
        self, mock_run_pipeline, mock_get_status, mock_defer
    ):
        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = AZ_PIPELINE_RUN_ID
        mock_run_pipeline.return_value = CreateRunResponse

        self.OPERATOR.execute(context=create_context(self.OPERATOR))
        assert not mock_defer.called

    @pytest.mark.parametrize("status", AzureDataFactoryPipelineRunStatus.FAILURE_STATES)
    @mock.patch(f"{MODULE}.AzureDataFactoryRunPipelineOperatorAsync.defer")
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_pipeline_run_status",
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
    def test_azure_data_factory_run_pipeline_operator_async_error_before_deferred(
        self, mock_run_pipeline, mock_get_status, mock_defer, status
    ):
        mock_get_status.return_value = status

        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = AZ_PIPELINE_RUN_ID
        mock_run_pipeline.return_value = CreateRunResponse

        with pytest.raises(AzureDataFactoryPipelineRunException):
            self.OPERATOR.execute(context=create_context(self.OPERATOR))
        assert not mock_defer.called

    @pytest.mark.parametrize("status", AzureDataFactoryPipelineRunStatus.INTERMEDIATE_STATES)
    @mock.patch(
        "airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.get_pipeline_run_status",
    )
    @mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
    def test_azure_data_factory_run_pipeline_operator_async(self, mock_run_pipeline, mock_get_status, status):
        """Assert that AzureDataFactoryRunPipelineOperatorAsync deferred"""

        class CreateRunResponse:
            pass

        CreateRunResponse.run_id = AZ_PIPELINE_RUN_ID
        mock_run_pipeline.return_value = CreateRunResponse

        with pytest.raises(TaskDeferred) as exc:
            self.OPERATOR.execute(context=create_context(self.OPERATOR))

        assert isinstance(
            exc.value.trigger, AzureDataFactoryTrigger
        ), "Trigger is not a AzureDataFactoryTrigger"

    def test_azure_data_factory_run_pipeline_operator_async_execute_complete_success(self):
        """Assert that execute_complete log success message"""

        with mock.patch.object(self.OPERATOR.log, "info") as mock_log_info:
            self.OPERATOR.execute_complete(
                context=create_context(self.OPERATOR),
                event={"status": "success", "message": "success", "run_id": AZ_PIPELINE_RUN_ID},
            )
        mock_log_info.assert_called_with("success")

    def test_azure_data_factory_run_pipeline_operator_async_execute_complete_fail(self):
        """Assert that execute_complete raise exception on error"""

        with pytest.raises(AirflowException):
            self.OPERATOR.execute_complete(
                context=create_context(self.OPERATOR),
                event={"status": "error", "message": "error", "run_id": AZ_PIPELINE_RUN_ID},
            )
