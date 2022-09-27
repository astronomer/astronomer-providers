"""
This is an example DAG which uses the LivyOperatorAsync.
The tasks below trigger the computation of pi on the Spark instance
using the Java and Python executables provided in the example library.
"""
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, List

from airflow import DAG, settings
from airflow.exceptions import AirflowException
from airflow.models import Connection, Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from botocore.exceptions import ClientError
from requests import get

from astronomer.providers.apache.livy.operators.livy import LivyOperatorAsync

BOTO_DUPLICATE_PERMISSION_ERROR = "InvalidPermission.Duplicate"
LIVY_JAVA_FILE = os.getenv("LIVY_JAVA_FILE", "/spark-examples.jar")
LIVY_OPERATOR_INGRESS_PORT = int(os.getenv("LIVY_OPERATOR_INGRESS_PORT", 8998))
LIVY_PYTHON_FILE = os.getenv("LIVY_PYTHON_FILE", "/user/hadoop/pi.py")
JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
PEM_FILENAME = os.getenv("PEM_FILENAME", "providers_team_keypair")
PRIVATE_KEY = Variable.get("providers_team_keypair")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

AWS_S3_CREDS = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "sample_aws_access_key_id"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "sample_aws_secret_access_key"),
    "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
}

COMMAND_TO_CREATE_PI_FILE: List[str] = [
    "curl https://raw.githubusercontent.com/apache/spark/master/examples/src/main/python/pi.py >> pi.py",
    "hadoop fs -copyFromLocal pi.py /user/hadoop",
]

JOB_FLOW_OVERRIDES = {
    "Name": "example_livy_operator_cluster",
    "ReleaseLabel": "emr-5.35.0",
    "Applications": [
        {"Name": "Spark"},
        {
            "Name": "Livy",
        },
        {
            "Name": "Hive",
        },
        {
            "Name": "Hadoop",
        },
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
        ],
        "Ec2KeyName": PEM_FILENAME,
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "Steps": [],
    "JobFlowRole": JOB_FLOW_ROLE,
    "ServiceRole": SERVICE_ROLE,
}


def create_airflow_connection(task_instance: Any) -> None:
    """
    Checks if airflow connection exists, if yes then deletes it.
    Then, create a new livy_default connection.
    """
    conn = Connection(
        conn_id="livy_default",
        conn_type="livy",
        host=task_instance.xcom_pull(
            key="cluster_response_master_public_dns", task_ids=["describe_created_cluster"]
        )[0],
        login="",
        password="",
        port=LIVY_OPERATOR_INGRESS_PORT,
    )  # create a connection object

    session = settings.Session()
    connection = session.query(Connection).filter_by(conn_id=conn.conn_id).one_or_none()
    if connection is None:
        logging.info("Connection %s doesn't exist.", str(conn.conn_id))
    else:
        session.delete(connection)
        session.commit()
        logging.info("Connection %s deleted.", str(conn.conn_id))

    session.add(conn)
    session.commit()  # it will insert the connection object programmatically.
    logging.info("Connection livy_default is created")


def add_inbound_rule_for_security_group(task_instance: Any) -> None:
    """
    Sets the inbound rule for the aws security group, based on
    current ip address of the system.
    """
    import boto3

    current_docker_ip = get("https://api.ipify.org").text
    logging.info("Current ip address is: %s", str(current_docker_ip))
    client = boto3.client("ec2", **AWS_S3_CREDS)

    # Open port 'LIVY_OPERATOR_INGRESS_PORT'.
    try:
        client.authorize_security_group_ingress(
            GroupId=task_instance.xcom_pull(
                key="cluster_response_master_security_group", task_ids=["describe_created_cluster"]
            )[0],
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": LIVY_OPERATOR_INGRESS_PORT,
                    "ToPort": LIVY_OPERATOR_INGRESS_PORT,
                    "IpRanges": [{"CidrIp": str(current_docker_ip) + "/32"}],
                }
            ],
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code", "") == BOTO_DUPLICATE_PERMISSION_ERROR:
            logging.error(
                "Ingress for port %s already authorized. Error Message is: %s",
                LIVY_OPERATOR_INGRESS_PORT,
                error.response["Error"]["Message"],
            )
        else:
            raise error

    # Open port 22 for downstream task 'ssh_and_copy_pifile_to_hdfs'.
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


def ssh_and_run_command(task_instance: Any, **kwargs: Any) -> None:
    """
    Load the private_key from airflow variable and creates a pem_file
    at /tmp/. SSH into the machine and execute the bash script from the list
    of commands.
    """
    # remove the file if it exists
    if os.path.exists(f"/tmp/{PEM_FILENAME}.pem"):
        os.remove(f"/tmp/{PEM_FILENAME}.pem")

    # read the content for pem file from Variable set on Airflow UI.
    with open(f"/tmp/{PEM_FILENAME}.pem", "w+") as fh:
        fh.write(PRIVATE_KEY)

    # write private key to file with 400 permissions
    os.chmod(f"/tmp/{PEM_FILENAME}.pem", 0o400)
    # check if the PEM file exists or not.
    if not os.path.exists(f"/tmp/{PEM_FILENAME}.pem"):
        # if it doesn't exists raise an error.
        raise AirflowException("PEM file wasn't copied properly.")

    import paramiko

    key = paramiko.RSAKey.from_private_key_file(kwargs["path_to_pem_file"])
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect/ssh to an instance
    cluster_response_master_public_dns = task_instance.xcom_pull(
        key="cluster_response_master_public_dns", task_ids=["describe_created_cluster"]
    )[0]
    try:
        client.connect(hostname=cluster_response_master_public_dns, username=kwargs["username"], pkey=key)

        # Execute a command(cmd) after connecting/ssh to an instance
        for command in kwargs["command"]:
            stdin, stdout, stderr = client.exec_command(command)
            stdout.read()

        # close the client connection once the job is done
        client.close()
    except Exception as exc:
        raise Exception("Got an exception as %s.", str(exc))


def get_cluster_details(task_instance: Any) -> None:
    """
    Fetches the cluster details and stores EmrManagedMasterSecurityGroup and
    MasterPublicDnsName in the XCOM.
    """
    import boto3

    client = boto3.client("emr", **AWS_S3_CREDS)
    response = client.describe_cluster(
        ClusterId=str(task_instance.xcom_pull(key="return_value", task_ids=["cluster_creator"])[0])
    )
    while (
        "MasterPublicDnsName" not in response["Cluster"]
        and response["Cluster"]["Status"]["State"] != "WAITING"
    ):
        logging.info("wait for 11 minutes to get the MasterPublicDnsName")
        time.sleep(660)
        response = client.describe_cluster(
            ClusterId=str(task_instance.xcom_pull(key="return_value", task_ids=["cluster_creator"])[0])
        )
        logging.info("current response from ams emr: %s", str(response))
    task_instance.xcom_push(
        key="cluster_response_master_public_dns", value=response["Cluster"]["MasterPublicDnsName"]
    )
    task_instance.xcom_push(
        key="cluster_response_master_security_group",
        value=response["Cluster"]["Ec2InstanceAttributes"]["EmrManagedMasterSecurityGroup"],
    )


default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}

with DAG(
    dag_id="example_livy_operator",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "livy"],
) as dag:
    # [START howto_operator_emr_create_job_flow]
    cluster_creator = EmrCreateJobFlowOperator(
        task_id="cluster_creator",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )
    # [END howto_operator_emr_create_job_flow]

    # [START describe_created_cluster]
    describe_created_cluster = PythonOperator(
        task_id="describe_created_cluster", python_callable=get_cluster_details
    )
    # [END describe_created_cluster]

    # [START add_example_pi_file_in_hdfs]
    ssh_and_copy_pifile_to_hdfs = PythonOperator(
        task_id="ssh_and_copy_pifile_to_hdfs",
        python_callable=ssh_and_run_command,
        op_kwargs={
            "path_to_pem_file": f"/tmp/{PEM_FILENAME}.pem",
            "username": "hadoop",
            "command": COMMAND_TO_CREATE_PI_FILE,
        },
    )
    # [END add_example_pi_file_in_hdfs]

    # [START add_ip_address_for_inbound_rules]
    get_and_add_ip_address_for_inbound_rules = PythonOperator(
        task_id="get_and_add_ip_address_for_inbound_rules",
        python_callable=add_inbound_rule_for_security_group,
    )
    # [END add_ip_address_for_inbound_rules]

    # [START create_airflow_connection_for_livy]
    create_airflow_connection_for_livy = PythonOperator(
        task_id="create_airflow_connection_for_livy", python_callable=create_airflow_connection
    )
    # [END create_airflow_connection_for_livy]

    # [START run_pi_example_without_polling_interval]
    livy_java_task = LivyOperatorAsync(
        task_id="livy_java_task",
        file=LIVY_JAVA_FILE,
        num_executors=1,
        conf={
            "spark.shuffle.compress": "false",
        },
        class_name="org.apache.spark.examples.SparkPi",
    )
    # [END run_pi_spark_without_polling_interval]

    # [START howto_operator_livy_async_with_polling_interval]
    livy_python_task = LivyOperatorAsync(
        task_id="livy_python_task", file=LIVY_PYTHON_FILE, polling_interval=30
    )
    # [END howto_operator_livy_async_with_polling_interval]

    # [START howto_operator_emr_terminate_job_flow]
    remove_cluster = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=str(cluster_creator.output),
        trigger_rule="all_done",
    )
    # [END howto_operator_emr_terminate_job_flow]

    (
        cluster_creator
        >> describe_created_cluster
        >> get_and_add_ip_address_for_inbound_rules
        >> ssh_and_copy_pifile_to_hdfs
        >> create_airflow_connection_for_livy
        >> livy_java_task
        >> livy_python_task
        >> remove_cluster
    )
