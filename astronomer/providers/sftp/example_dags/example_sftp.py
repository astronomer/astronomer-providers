import logging
import os
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.timezone import datetime
from airflow.utils.trigger_rule import TriggerRule
from requests import get

from astronomer.providers.sftp.sensors.sftp import SFTPSensorAsync

if TYPE_CHECKING:
    from airflow.models import TaskInstance

SFTP_CONN_ID = os.getenv("ASTRO_SFTP_CONN_ID", "sftp_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
AWS_S3_CREDS = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "aws_access_key"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "aws_secret_key"),
    "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
}
AMI_ID = os.getenv("AMI_ID", "ami-097a2df4ac947655f")
PEM_FILENAME = os.getenv("PEM_FILENAME", "providers_team_keypair")
BOTO_DUPLICATE_PERMISSION_ERROR = "InvalidPermission.Duplicate"

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def create_ec2_instance() -> None:
    """Create ec2 instance"""
    import boto3

    ec2 = boto3.resource("ec2", **AWS_S3_CREDS)
    instance = ec2.create_instances(
        ImageId=AMI_ID, MinCount=1, MaxCount=1, InstanceType="t2.micro", KeyName=PEM_FILENAME
    )
    instance_id = instance[0].id
    ti = get_current_context()["ti"]
    ti.xcom_push(key="ec2_instance_id", value=instance_id)


def terminate_instance(task_instance: "TaskInstance") -> None:
    """Terminate ec2 instance by instance id"""
    import boto3

    ec2 = boto3.client("ec2", **AWS_S3_CREDS)
    ec2_instance_id_xcom = task_instance.xcom_pull(key="ec2_instance_id", task_ids=["create_ec2_instance"])[0]
    ec2.terminate_instances(
        InstanceIds=[
            ec2_instance_id_xcom,
        ],
    )


def add_inbound_rule_for_security_group(task_instance: Any) -> None:
    """
    Sets the inbound rule for the aws security group, based on
    current ip address of the system.
    """
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("ec2", **AWS_S3_CREDS)
    current_docker_ip = get("https://api.ipify.org").text

    # Allow SSH traffic on port 22 and copy file to HDFS.
    try:
        client.authorize_security_group_ingress(
            GroupId=task_instance.xcom_pull(
                key="cluster_response_master_security_group", task_ids=["describe_created_cluster"]
            )[0],
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": 22,
                    "ToPort": 22,
                    "IpRanges": [{"CidrIp": str(current_docker_ip) + "/32"}],
                }
            ],
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code", "") == BOTO_DUPLICATE_PERMISSION_ERROR:
            logging.error(
                "Ingress for port 22 already authorized. Error message is: %s",
                error.response["Error"]["Message"],
            )
        else:
            raise error


with DAG(
    dag_id="example_async_sftp_sensor",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "sftp"],
) as dag:

    cluster_creator = PythonOperator(task_id="create_ec2_instance", python_callable=create_ec2_instance)

    get_and_add_ip_address_for_inbound_rules = PythonOperator(
        task_id="get_and_add_ip_address_for_inbound_rules",
        python_callable=add_inbound_rule_for_security_group,
    )

    # [START howto_sensor_sftp_async]
    async_sftp_sensor = SFTPSensorAsync(
        task_id="async_sftp_sensor",
        sftp_conn_id=SFTP_CONN_ID,
        path="path/on/sftp/server",
        file_pattern="*.csv",
        poke_interval=5,
    )
    # [END howto_sensor_sftp_async]

    terminate_ec2_instance = PythonOperator(
        task_id="terminate_instance", trigger_rule=TriggerRule.ALL_DONE, python_callable=terminate_instance
    )

    cluster_creator >> async_sftp_sensor >> get_and_add_ip_address_for_inbound_rules >> terminate_ec2_instance
