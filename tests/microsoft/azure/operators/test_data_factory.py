from datetime import datetime
from unittest import mock

import pytest
from airflow import DAG
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.types import DagRunType

from astronomer.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperatorAsync,
)
from astronomer.providers.microsoft.azure.triggers.data_factory import (
    AzureDataFactoryTrigger,
)

AZ_PIPELINE_RUN_ID = "123"


def create_context(task):
    dag = DAG(dag_id="dag")
    execution_date = datetime(2022, 1, 1, 0, 0, 0)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch("airflow.providers.microsoft.azure.hooks.data_factory.AzureDataFactoryHook.run_pipeline")
def test_azure_data_factory_run_pipeline_operator_async(mock_run_pipeline):
    """Assert that AzureDataFactoryRunPipelineOperatorAsync deferred"""

    class CreateRunResponse(object):
        pass

    CreateRunResponse.run_id = AZ_PIPELINE_RUN_ID
    mock_run_pipeline.return_value = CreateRunResponse
    op = AzureDataFactoryRunPipelineOperatorAsync(
        task_id="run_pipeline",
        pipeline_name="pipeline",
        parameters={"myParam": "value"},
    )
    with pytest.raises(TaskDeferred) as exc:
        op.execute(context=create_context(op))

    assert isinstance(exc.value.trigger, AzureDataFactoryTrigger), "Trigger is not a AzureDataFactoryTrigger"


def test_azure_data_factory_run_pipeline_operator_async_execute_complete_success():
    """Assert that execute_complete log success message"""
    op = AzureDataFactoryRunPipelineOperatorAsync(
        task_id="run_pipeline",
        pipeline_name="pipeline",
        parameters={"myParam": "value"},
    )

    with mock.patch.object(op.log, "info") as mock_log_info:
        op.execute_complete(
            context=create_context(op),
            event={"status": "success", "message": "success", "run_id": AZ_PIPELINE_RUN_ID},
        )
    mock_log_info.assert_called_with("success")


def test_azure_data_factory_run_pipeline_operator_async_execute_complete_fail():
    """Assert that execute_complete raise exception on error"""
    op = AzureDataFactoryRunPipelineOperatorAsync(
        task_id="run_pipeline",
        pipeline_name="pipeline",
        parameters={"myParam": "value"},
    )

    with pytest.raises(AirflowException):
        op.execute_complete(
            context=create_context(op),
            event={"status": "error", "message": "error", "run_id": AZ_PIPELINE_RUN_ID},
        )
