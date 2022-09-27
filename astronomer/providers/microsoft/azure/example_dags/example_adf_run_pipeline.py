import logging
import os
import time
from datetime import datetime, timedelta
from typing import cast

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator

from astronomer.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperatorAsync,
)
from astronomer.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensorAsync,
)

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "azure_data_factory_conn_id": "azure_data_factory_default",
    "factory_name": "ADFProvidersTeamDataFactoryTest",  # This can also be specified in the ADF connection.
    "resource_group_name": "team_provider_resource_group_test_1",  # This can also be specified in the ADF connection.
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
TENANT_ID = os.getenv("TENANT_ID", "")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "")
RESOURCE_GROUP_NAME = os.getenv("RESOURCE_GROUP_NAME", "")
DATAFACTORY_NAME = os.getenv("DATAFACTORY_NAME", "")
LOCATION = os.getenv("LOCATION", "eastus")
CONNECTION_STRING = os.getenv("CONNECTION_STRING", "")
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "pipeline1")
ACTIVITY_NAME = os.getenv("ACTIVITY_NAME", "copyBlobtoBlob")
DATASET_INPUT_NAME = os.getenv("DATASET_INPUT_NAME", "ds_in")
DATASET_OUTPUT_NAME = os.getenv("DATASET_OUTPUT_NAME", "ds_out")
BLOB_FILE_NAME = os.getenv("BLOB_FILE_NAME", "test.txt")
OUTPUT_BLOB_PATH = os.getenv("OUTPUT_BLOB_PATH", "container1/output")
BLOB_PATH = os.getenv("BLOB_PATH", "container1/input")
STORAGE_LINKED_SERVICE_NAME = os.getenv("STORAGE_LINKED_SERVICE_NAME", "storageLinkedService001")
rg_params = {"location": LOCATION}
df_params = {"location": LOCATION}


def create_adf_storage_pipeline() -> None:
    """
    Creates Azure resource if not present, Azure Data factory, Azure Storage linked service,
    Azure blob dataset both input and output and Data factory pipeline
    """
    from azure.core.exceptions import HttpResponseError
    from azure.identity import ClientSecretCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.datafactory.models import (
        AzureBlobDataset,
        AzureStorageLinkedService,
        BlobSink,
        BlobSource,
        CopyActivity,
        DatasetReference,
        DatasetResource,
        Factory,
        LinkedServiceReference,
        LinkedServiceResource,
        PipelineResource,
        SecureString,
    )
    from azure.mgmt.resource import ResourceManagementClient

    credentials = ClientSecretCredential(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET, tenant_id=TENANT_ID
    )
    resource_client = ResourceManagementClient(credentials, SUBSCRIPTION_ID)
    resource_group_exist = None
    try:
        resource_group_exist = resource_client.resource_groups.get(RESOURCE_GROUP_NAME)
    except HttpResponseError as e:
        logging.exception("Resource group not found, so creating one %s", e.__str__())
    if not resource_group_exist:
        resource_client.resource_groups.create_or_update(RESOURCE_GROUP_NAME, rg_params)

    # Create a data factory
    adf_client = DataFactoryManagementClient(credentials, SUBSCRIPTION_ID)
    df_resource = Factory(location=LOCATION)
    df = adf_client.factories.create_or_update(RESOURCE_GROUP_NAME, DATAFACTORY_NAME, df_resource)
    while df.provisioning_state != "Succeeded":
        df = adf_client.factories.get(RESOURCE_GROUP_NAME, DATAFACTORY_NAME)
        time.sleep(1)

    # Create an Azure Storage linked service

    # IMPORTANT: specify the name and key of your Azure Storage account.
    storage_string = SecureString(value=CONNECTION_STRING)

    ls_azure_storage = LinkedServiceResource(
        properties=AzureStorageLinkedService(connection_string=storage_string)
    )
    adf_client.linked_services.create_or_update(
        RESOURCE_GROUP_NAME, DATAFACTORY_NAME, STORAGE_LINKED_SERVICE_NAME, ls_azure_storage
    )

    # Create an Azure blob dataset (input)
    ds_ls = LinkedServiceReference(reference_name=STORAGE_LINKED_SERVICE_NAME)
    ds_azure_blob = DatasetResource(
        properties=AzureBlobDataset(
            linked_service_name=ds_ls, folder_path=BLOB_PATH, file_name=BLOB_FILE_NAME
        )
    )
    adf_client.datasets.create_or_update(
        RESOURCE_GROUP_NAME, DATAFACTORY_NAME, DATASET_INPUT_NAME, ds_azure_blob
    )

    # Create an Azure blob dataset (output)
    ds_out_azure_blob = DatasetResource(
        properties=AzureBlobDataset(linked_service_name=ds_ls, folder_path=OUTPUT_BLOB_PATH)
    )
    adf_client.datasets.create_or_update(
        RESOURCE_GROUP_NAME, DATAFACTORY_NAME, DATASET_OUTPUT_NAME, ds_out_azure_blob
    )

    # Create a copy activity
    blob_source = BlobSource()
    blob_sink = BlobSink()
    ds_in_ref = DatasetReference(reference_name=DATASET_INPUT_NAME)
    ds_out_ref = DatasetReference(reference_name=DATASET_OUTPUT_NAME)
    copy_activity = CopyActivity(
        name=ACTIVITY_NAME, inputs=[ds_in_ref], outputs=[ds_out_ref], source=blob_source, sink=blob_sink
    )

    # Create a pipeline with the copy activity
    p_obj = PipelineResource(activities=[copy_activity], parameters={})
    adf_client.pipelines.create_or_update(RESOURCE_GROUP_NAME, DATAFACTORY_NAME, PIPELINE_NAME, p_obj)


def delete_azure_data_factory_storage_pipeline() -> None:
    """Delete data factory, storage linked service pipeline, dataset"""
    from azure.identity import ClientSecretCredential
    from azure.mgmt.datafactory import DataFactoryManagementClient
    from azure.mgmt.resource import ResourceManagementClient

    credentials = ClientSecretCredential(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET, tenant_id=TENANT_ID
    )
    # create resource client
    resource_client = ResourceManagementClient(credentials, SUBSCRIPTION_ID)

    # create Data factory client
    adf_client = DataFactoryManagementClient(credentials, SUBSCRIPTION_ID)

    # Delete pipeline
    adf_client.pipelines.delete(RESOURCE_GROUP_NAME, DATAFACTORY_NAME, PIPELINE_NAME)

    # Delete input dataset
    adf_client.datasets.delete(RESOURCE_GROUP_NAME, DATAFACTORY_NAME, DATASET_INPUT_NAME)

    # Delete output dataset
    adf_client.datasets.delete(RESOURCE_GROUP_NAME, DATAFACTORY_NAME, DATASET_OUTPUT_NAME)

    # Delete Linked services
    adf_client.linked_services.delete(
        RESOURCE_GROUP_NAME, DATAFACTORY_NAME, linked_service_name=STORAGE_LINKED_SERVICE_NAME
    )

    # Delete Data factory
    adf_client.factories.delete(RESOURCE_GROUP_NAME, DATAFACTORY_NAME)

    # Delete Resource Group
    resource_client.resource_groups.begin_delete(RESOURCE_GROUP_NAME)


with DAG(
    dag_id="example_async_adf_run_pipeline",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "Azure Pipeline"],
) as dag:
    # [START howto_create_resource_group]
    create_azure_data_factory_storage_pipeline = PythonOperator(
        task_id="create_azure_data_factory_storage_pipeline",
        python_callable=create_adf_storage_pipeline,
    )
    # [END howto_create_resource_group]

    # [START howto_operator_adf_run_pipeline_async]
    run_pipeline_wait = AzureDataFactoryRunPipelineOperatorAsync(
        task_id="run_pipeline_wait",
        pipeline_name=PIPELINE_NAME,
    )
    # [END howto_operator_adf_run_pipeline_async]

    # [START howto_operator_adf_run_pipeline]
    run_pipeline_no_wait = AzureDataFactoryRunPipelineOperatorAsync(
        task_id="run_pipeline_no_wait",
        pipeline_name=PIPELINE_NAME,
        wait_for_termination=False,
    )
    # [END howto_operator_adf_run_pipeline]

    # [START howto_sensor_pipeline_run_sensor_async]
    pipeline_run_sensor_async = AzureDataFactoryPipelineRunStatusSensorAsync(
        task_id="pipeline_run_sensor_async",
        run_id=cast(str, XComArg(run_pipeline_wait, key="run_id")),
    )
    # [END howto_sensor_pipeline_run_sensor_async]

    remove_azure_data_factory_storage_pipeline = PythonOperator(
        task_id="remove_azure_data_factory_storage_pipeline",
        python_callable=delete_azure_data_factory_storage_pipeline,
        trigger_rule="all_done",
    )

    (
        create_azure_data_factory_storage_pipeline
        >> run_pipeline_wait
        >> run_pipeline_no_wait
        >> pipeline_run_sensor_async
        >> remove_azure_data_factory_storage_pipeline
    )
