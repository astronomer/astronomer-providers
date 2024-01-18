from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator

from astronomer.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperatorAsync,
)


class TestAzureDataFactoryRunPipelineOperatorAsync:
    def test_init(self):
        task = AzureDataFactoryRunPipelineOperatorAsync(
            task_id="run_pipeline",
            pipeline_name="pipeline",
            parameters={"myParam": "value"},
            factory_name="factory_name",
            resource_group_name="resource_group",
        )

        assert isinstance(task, AzureDataFactoryRunPipelineOperator)
        assert task.deferrable is True
