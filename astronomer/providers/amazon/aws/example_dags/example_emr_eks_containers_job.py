import logging
import os
import time
from datetime import datetime, timedelta
from typing import Optional

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator
from botocore.exceptions import ClientError

from astronomer.providers.amazon.aws.sensors.emr import EmrContainerSensorAsync

VIRTUAL_CLUSTER_ID = os.getenv("VIRTUAL_CLUSTER_ID", "xxxxxxx")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::475538383708:role/test_job_execution_role")
# [END howto_operator_emr_eks_env_variables]

EKS_CONTAINER_PROVIDER_CLUSTER_NAME = os.getenv(
    "EKS_CONTAINER_PROVIDER_CLUSTER_NAME", "providers-team-eks-cluster"
)
KUBECTL_CLUSTER_NAME = os.getenv("KUBECTL_CLUSTER_NAME", "providers-team-eks-namespace")
VIRTUAL_CLUSTER_NAME = os.getenv("EMR_VIRTUAL_CLUSTER_NAME", "providers-team-virtual-eks-cluster")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "xxxxxxx")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "xxxxxxxx")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")

default_args = {
    "execution_timeout": timedelta(minutes=30),
}


def get_virtual_cluster_status(virtual_cluster_id) -> str:
    """Get the virtual cluster status"""
    boto_client = boto3.client("emr-containers")

    cluster_response = boto_client.describe_virtual_cluster(
        id=virtual_cluster_id,
    )
    logging.info("%s", cluster_response)
    virtual_cluster = cluster_response.get("virtualCluster")
    cluster_state: str = virtual_cluster.get("state")
    return cluster_state


def delete_emr_container_virtual_cluster_by_id(virtual_cluster_id) -> Optional[str]:
    """Deletes an EMR on EKS virtual cluster"""
    emr_container_client = boto3.client("emr-containers")

    try:
        emr_container_client.delete_virtual_cluster(id=virtual_cluster_id)
        while True:
            virtual_cluster_state = get_virtual_cluster_status(virtual_cluster_id)
            if virtual_cluster_state == "TERMINATING":
                time.sleep(30)
                continue
            elif virtual_cluster_state == "TERMINATED":
                return virtual_cluster_state
    except ClientError:
        logging.exception("Error when deleting the cluster")
        return None


def get_virtual_cluster_id():
    """Get list of virtual cluster in container"""
    emr_client = boto3.client("emr-containers")
    try:
        list_cluster_response = emr_client.list_virtual_clusters(
            containerProviderId=EKS_CONTAINER_PROVIDER_CLUSTER_NAME,
            containerProviderType="EKS",
            states=[
                "RUNNING",
            ],
            maxResults=123,
        )
        for cluster in list_cluster_response["virtualClusters"]:
            if cluster["name"] == VIRTUAL_CLUSTER_NAME:
                return cluster
        return None
    except ClientError:
        logging.exception("Error when getting the list of virtual cluster ")
        return None


def delete_eks_cluster():
    """Delete EKS cluster"""
    try:
        client = boto3.client("eks")
        status = client.describe_cluster(name=EKS_CONTAINER_PROVIDER_CLUSTER_NAME)
        if status and status == "ACTIVE":
            client.delete_cluster(name=EKS_CONTAINER_PROVIDER_CLUSTER_NAME)
            while True:
                status = client.describe_cluster(name=EKS_CONTAINER_PROVIDER_CLUSTER_NAME)
                if status == "DELETING":
                    time.sleep(30)
                    continue
    except ClientError:
        logging.exception("Error while deleting EKS cluster")


def delete_cluster():
    """Delete EMR, EKS and virtual cluster"""
    # delete virtual cluster
    virtual_cluster_details = get_virtual_cluster_id()
    if virtual_cluster_details and virtual_cluster_details["state"] == "RUNNING":
        delete_emr_container_virtual_cluster_by_id(virtual_cluster_details["id"])

    # delete eks cluster
    delete_eks_cluster()


def create_emr_virtual_cluster_func():
    """Create EMR virtual cluster in container"""
    client = boto3.client("emr-containers")
    try:
        response = client.create_virtual_cluster(
            name=VIRTUAL_CLUSTER_NAME,
            containerProvider={
                "id": EKS_CONTAINER_PROVIDER_CLUSTER_NAME,
                "type": "EKS",
                "info": {"eksInfo": {"namespace": KUBECTL_CLUSTER_NAME}},
            },
        )
        os.environ["VIRTUAL_CLUSTER_ID"] = response["id"]
    except ClientError:
        logging.exception("Error while creating EMR virtual cluster")
        return None


# [START howto_operator_emr_eks_config]
JOB_DRIVER_ARG = {
    "sparkSubmitJobDriver": {
        "entryPoint": "local:///usr/lib/spark/examples/src/main/python/pi.py",
        "sparkSubmitParameters": "--conf spark.executors.instances=2 --conf spark.executors.memory=2G --conf spark.executor.cores=2 --conf spark.driver.cores=1",  # noqa: E501
    }
}

CONFIGURATION_OVERRIDES_ARG = {
    "applicationConfiguration": [
        {
            "classification": "spark-defaults",
            "properties": {
                "spark.hadoop.hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",  # noqa: E501
            },
        }
    ],
    "monitoringConfiguration": {
        "cloudWatchMonitoringConfiguration": {
            "logGroupName": "/aws/emr-eks-spark",
            "logStreamNamePrefix": "airflow",
        }
    },
}
# [END howto_operator_emr_eks_config]

with DAG(
    dag_id="emr_eks_pi_job",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["emr_containers", "example"],
) as dag:
    # Task steps for DAG to be self-sufficient
    setup_aws_config = BashOperator(
        task_id="setup_aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    # Delete clusters, container providers
    delete_existing_emr_virtual_cluster_container = PythonOperator(
        task_id="delete_existing_emr_virtual_cluster_container",
        python_callable=delete_cluster,
    )

    # Task to create EMR clusters on EKS
    create_EKS_cluster_kube_namespace = BashOperator(
        task_id="create_EKS_cluster_kube_namespace",
        bash_command="sh /usr/local/airflow/dags/example_create_emr_on_eks_cluster.sh ",
    )

    # Task to create EMR virtual cluster
    create_EMR_virtual_cluster = PythonOperator(
        task_id="create_EMR_virtual_cluster",
        python_callable=create_emr_virtual_cluster_func,
    )

    # An example of how to get the cluster id and arn from an Airflow connection
    # VIRTUAL_CLUSTER_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
    # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'

    # [START howto_operator_emr_eks_jobrun]
    job_starter = EmrContainerOperator(
        task_id="start_job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        execution_role_arn=JOB_ROLE_ARN,
        release_label="emr-6.2.0-latest",
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        name="pi.py",
    )
    # [END howto_operator_emr_eks_jobrun]

    # [START howto_sensor_emr_container_task]
    job_container_sensor = EmrContainerSensorAsync(
        task_id="check_container_sensor",
        job_id=job_starter.output,
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        poll_interval=5,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_emr_container_task]

    # Delete clusters, container providers
    last_step_delete_cluster = PythonOperator(
        task_id="last_step_delete_cluster",
        python_callable=delete_cluster,
    )

    (
        setup_aws_config
        >> delete_existing_emr_virtual_cluster_container
        >> create_EKS_cluster_kube_namespace
        >> create_EMR_virtual_cluster
        >> job_starter
        >> job_container_sensor
        >> last_step_delete_cluster
    )
