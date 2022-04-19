import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)

default_args = {
    "execution_timeout": timedelta(minutes=30),
    "azure_data_factory_conn_id": "azure_data_factory",
}

SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "")
RESOURCE_GROUP_NAME = os.getenv("REGION_NAME", "")
DATAFACTORY_NAME = os.getenv("DATAFACTORY_NAME", "")

with DAG(
    dag_id="example_adf_run_pipeline",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "Azure Pipeline"],
) as dag:
    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    # [START howto_operator_adf_run_pipeline]
    run_pipeline1 = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline1",
        pipeline_name="pipeline1",
        parameters={"myParam": "value"},
    )
    # [END howto_operator_adf_run_pipeline]

    # pipeline_run_sensor = AzureDataFactoryPipelineRunStatusSensorAsync(
    #     task_id="pipeline_run_sensor",
    #     run_id=run_pipeline1.output["run_id"],
    # )
    # [END howto_operator_adf_run_pipeline_async]

    run_pipeline1 >> end

    # Task dependency created via `XComArgs`:
    #   run_pipeline2 >> pipeline_run_sensor
