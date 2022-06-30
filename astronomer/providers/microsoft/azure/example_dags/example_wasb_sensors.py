import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from astronomer.providers.microsoft.azure.sensors.wasb import (
    WasbBlobSensorAsync,
    WasbPrefixSensorAsync,
)

AZURE_DATA_STORAGE_BLOB_NAME = os.getenv("AZURE_DATA_STORAGE_BLOB_NAME", "test_blob_providers_team.txt")
AZURE_DATA_STORAGE_CONTAINER_NAME = os.getenv(
    "AZURE_DATA_STORAGE_CONTAINER_NAME", "test-container-providers-team"
)
AZURE_WASB_CONN_ID = os.getenv("AZURE_WASB_CONN_ID", "wasb_default")
EXAMPLE_AZURE_TEST_FILE_PATH = "/usr/local/airflow/dags/example_azure_test_file.txt"
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 10))),
}


def create_azure_data_storage_container_callable(container_name: str) -> None:
    """
    Creates an Azure Data Storage container.

    If a container with the given name already exists, it just logs saying that and does not raise an Exception.

    :param container_name: name of the data storage container to be created
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    hook = WasbHook(wasb_conn_id=AZURE_WASB_CONN_ID)
    hook.create_container(container_name)


def delete_azure_data_storage_container_callable(container_name: str) -> None:
    """
    Deletes the Azure Data Storage container.

    :param container_name: name of the data storage container to be deleted
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    hook = WasbHook(wasb_conn_id=AZURE_WASB_CONN_ID)
    hook.delete_container(container_name)


def upload_blob_callable(file_path: str, container_name: str, blob_name: str) -> None:
    """
    Uploads the given example file as a blob in the given container.

    It raises an exception if the specified name of the blob is less than 10 characters with an interest to test
    the WASB Prefix Sensor successfully.

    :param file_path: local file path to be uploaded
    :param container_name: name of the data storage container in which the blob needs to be uploaded
    :param blob_name: name of the blob the file should be uploaded as in the container
    """
    if len(blob_name) < 10:
        raise ValueError("Blob name of least 10 characters long is recommended to test the 'Prefix Sensor'")

    import logging

    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    from azure.core.exceptions import ResourceExistsError

    hook = WasbHook(wasb_conn_id=AZURE_WASB_CONN_ID)
    try:
        hook.load_file(file_path, container_name, blob_name)
    except ResourceExistsError:
        logging.warning("Blob exists already")


def delete_blob_callable(container_name: str, blob_name: str) -> None:
    """
    Deletes the blob from the given container.

    :param container_name: name of the container in which to search for the blob to be deleted
    :param blob_name: name of the blob to be deleted
    """
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    hook = WasbHook(wasb_conn_id=AZURE_WASB_CONN_ID)
    hook.delete_file(container_name, blob_name)


with DAG(
    dag_id="example_wasb_sensors",
    start_date=datetime(2021, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "Azure Pipeline"],
) as dag:

    create_data_storage_container = PythonOperator(
        task_id="create_data_storage_container",
        python_callable=create_azure_data_storage_container_callable,
        op_args=[AZURE_DATA_STORAGE_CONTAINER_NAME],
    )

    upload_blob = PythonOperator(
        task_id="upload_blob",
        python_callable=upload_blob_callable,
        op_args=[
            EXAMPLE_AZURE_TEST_FILE_PATH,
            AZURE_DATA_STORAGE_CONTAINER_NAME,
            AZURE_DATA_STORAGE_BLOB_NAME,
        ],
    )

    # [START howto_sensor_wasb_blob_sensor_async]
    wasb_blob_sensor = WasbBlobSensorAsync(
        task_id="wasb_blob_sensor",
        container_name=AZURE_DATA_STORAGE_CONTAINER_NAME,
        blob_name=AZURE_DATA_STORAGE_BLOB_NAME,
        wasb_conn_id=AZURE_WASB_CONN_ID,
    )
    # [END howto_sensor_wasb_blob_sensor_async]

    # [START howto_sensor_wasb_prefix_sensor_async]
    wasb_prefix_sensor = WasbPrefixSensorAsync(
        task_id="wasb_prefix_sensor",
        container_name=AZURE_DATA_STORAGE_CONTAINER_NAME,
        prefix=AZURE_DATA_STORAGE_BLOB_NAME[:10],
        wasb_conn_id=AZURE_WASB_CONN_ID,
    )
    # [END howto_sensor_wasb_prefix_sensor_async]

    delete_blob = PythonOperator(
        task_id="delete_blob",
        python_callable=delete_blob_callable,
        op_args=[AZURE_DATA_STORAGE_CONTAINER_NAME, AZURE_DATA_STORAGE_BLOB_NAME],
        trigger_rule="all_done",
    )

    delete_data_storage_container = PythonOperator(
        task_id="delete_data_storage_container",
        python_callable=delete_azure_data_storage_container_callable,
        op_args=[AZURE_DATA_STORAGE_CONTAINER_NAME],
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    create_data_storage_container >> upload_blob
    upload_blob >> [wasb_blob_sensor, wasb_prefix_sensor]
    [wasb_blob_sensor, wasb_prefix_sensor] >> delete_blob
    delete_blob >> delete_data_storage_container

    [
        create_data_storage_container,
        upload_blob,
        wasb_blob_sensor,
        wasb_prefix_sensor,
        delete_blob,
        delete_data_storage_container,
    ] >> end
