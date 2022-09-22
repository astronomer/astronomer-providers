"""This is an example dag for hive partition sensors."""
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
from airflow.providers.apache.hive.operators.hive import HiveOperator
from requests import get

from astronomer.providers.apache.hive.sensors.hive_partition import (
    HivePartitionSensorAsync,
)
from astronomer.providers.apache.hive.sensors.named_hive_partition import (
    NamedHivePartitionSensorAsync,
)

AWS_S3_CREDS = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "aws_access_key"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "aws_secret_key"),
    "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
}
BOTO_DUPLICATE_PERMISSION_ERROR = "InvalidPermission.Duplicate"
PEM_FILENAME = os.getenv("PEM_FILENAME", "providers_team_keypair")
PRIVATE_KEY = Variable.get("providers_team_keypair")
HIVE_OPERATOR_INGRESS_PORT = int(os.getenv("HIVE_OPERATOR_INGRESS_PORT", 10000))
HIVE_SCHEMA = os.getenv("HIVE_SCHEMA", "default")
HIVE_TABLE = os.getenv("HIVE_TABLE", "zipcode")
HIVE_PARTITION = os.getenv("HIVE_PARTITION", "state='FL'")
JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

COMMAND_TO_CREATE_TABLE_DATA_FILE: List[str] = [
    "curl https://raw.githubusercontent.com/astronomer/astronomer-providers/\
main/astronomer/providers/apache/hive/example_dags/zipcodes.csv \
 >> zipcodes.csv",
    "hdfs dfs -put zipcodes.csv /user/root",
]

# This example uses an emr-5.34.0 cluster, apache-hive-2.3.9 and hadoop-2.10.1.
# If you would like to use a different version of the EMR cluster, then we need to
# match the hive and hadoop versions same as specified in the integration tests `Dockefile`.
JOB_FLOW_OVERRIDES = {
    "Name": "example_hive_sensor_cluster",
    "ReleaseLabel": "emr-5.34.0",
    "Applications": [
        {"Name": "Spark"},
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


def create_airflow_connection_for_hive_metastore_(task_instance: Any) -> None:
    """
    Checks if airflow connection exists, if yes then deletes it.
    Then, create a new metastore_default connection.
    """
    conn = Connection(
        conn_id="metastore_default",
        conn_type="hive_metastore",
        host=task_instance.xcom_pull(
            key="cluster_response_master_public_dns", task_ids=["describe_created_cluster"]
        )[0],
        login="hive",
        password="password",
        port=HIVE_OPERATOR_INGRESS_PORT,
        schema="default",
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
    logging.info("Connection metastore_default is created")


def create_airflow_connection_for_hive_cli(task_instance: Any) -> None:
    """
    Checks if airflow connection exists, if yes then deletes it.
    Then, create a new metastore_default connection.
    """
    conn = Connection(
        conn_id="hive_cli_default",
        conn_type="hive_cli",
        host=task_instance.xcom_pull(
            key="cluster_response_master_public_dns", task_ids=["describe_created_cluster"]
        )[0],
        login="hive",
        password="password",
        port=HIVE_OPERATOR_INGRESS_PORT,
        schema="default",
        extra={"use_beeline": True, "auth": "none"},
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
    logging.info("Connection metastore_default is created")


def add_inbound_rule_for_security_group(task_instance: Any) -> None:
    """
    Sets the inbound rule for the aws security group, based on
    current ip address of the system.
    """
    import boto3
    from botocore.exceptions import ClientError

    current_docker_ip = get("https://api.ipify.org").text
    logging.info("Current ip address is: %s", str(current_docker_ip))
    client = boto3.client("ec2", **AWS_S3_CREDS)

    # Port HIVE_OPERATOR_INGRESS_PORT needs to be open for Impyla connectivity to Hive.
    try:
        client.authorize_security_group_ingress(
            GroupId=task_instance.xcom_pull(
                key="cluster_response_master_security_group", task_ids=["describe_created_cluster"]
            )[0],
            IpPermissions=[
                {
                    "IpProtocol": "tcp",
                    "FromPort": HIVE_OPERATOR_INGRESS_PORT,
                    "ToPort": HIVE_OPERATOR_INGRESS_PORT,
                    "IpRanges": [{"CidrIp": str(current_docker_ip) + "/32"}],
                }
            ],
        )
    except ClientError as error:
        if error.response.get("Error", {}).get("Code", "") == BOTO_DUPLICATE_PERMISSION_ERROR:
            logging.error(
                "Ingress for port %s already authorized. Error Message is: %s",
                HIVE_OPERATOR_INGRESS_PORT,
                error.response["Error"]["Message"],
            )
        else:
            raise error

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
    # Check if the PEM file exists or not.
    if not os.path.exists(f"/tmp/{PEM_FILENAME}.pem"):
        # if it doesn't exists raise an error
        raise AirflowException("PEM file wasn't copied properly.")

    import paramiko

    key = paramiko.RSAKey.from_private_key_file(kwargs["path_to_pem_file"])
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect/ssh to an instance
    cluster_response_master_public_dns = task_instance.xcom_pull(
        key="cluster_response_master_public_dns", task_ids=["describe_created_cluster"]
    )[0]
    client.connect(hostname=cluster_response_master_public_dns, username=kwargs["username"], pkey=key)

    # Execute a command(cmd) after connecting/ssh to an instance
    for command in kwargs["command"]:
        stdin, stdout, stderr = client.exec_command(command)
        stdout.read()

    # close the client connection once the job is done
    client.close()


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
    dag_id="example_hive_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "hive", "hive_partition"],
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

    # [START add_ip_address_for_inbound_rules]
    get_and_add_ip_address_for_inbound_rules = PythonOperator(
        task_id="get_and_add_ip_address_for_inbound_rules",
        python_callable=add_inbound_rule_for_security_group,
    )
    # [END add_ip_address_for_inbound_rules]

    # [START add_example_pi_file_in_hdfs]
    ssh_and_copy_pifile_to_hdfs = PythonOperator(
        task_id="ssh_and_copy_pifile_to_hdfs",
        python_callable=ssh_and_run_command,
        op_kwargs={
            "path_to_pem_file": f"/tmp/{PEM_FILENAME}.pem",
            "username": "hadoop",
            "command": COMMAND_TO_CREATE_TABLE_DATA_FILE,
        },
    )
    # [END add_example_pi_file_in_hdfs]

    # [START create_hive_cli_airflow_connection]
    create_hive_cli_airflow_connection = PythonOperator(
        task_id="create_hive_cli_airflow_connection", python_callable=create_airflow_connection_for_hive_cli
    )
    # [END create_hive_cli_airflow_connection]

    # [START create_hive_metastore_airflow_connection]
    create_hive_metastore_airflow_connection = PythonOperator(
        task_id="create_hive_metastore_airflow_connection",
        python_callable=create_airflow_connection_for_hive_metastore_,
    )
    # [END create_hive_metastore_airflow_connection]

    # [START load_to_hive]
    load_to_hive = HiveOperator(
        task_id="hive_query",
        hql=(
            "CREATE TABLE zipcode(RecordNumber int,Country string,City string,Zipcode int) "
            "PARTITIONED BY(state string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
            "CREATE TABLE zipcodes_tmp(RecordNumber int,Country string,City string,Zipcode int,state string)"
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
            "LOAD DATA INPATH '/user/root/zipcodes.csv' INTO TABLE zipcodes_tmp;"
            "SET hive.exec.dynamic.partition.mode = nonstrict;"
            "INSERT into zipcode PARTITION(state) SELECT * from  zipcodes_tmp;"
        ),
    )
    # [END load_to_hive]

    # [START howto_sensor_hive_partition]
    hive_sensor = HivePartitionSensorAsync(
        task_id="hive_partition_check",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION,
        poke_interval=5,
    )
    # [END howto_sensor_hive_partition]

    # [START howto_sensor_named_hive_partition_async]
    wait_for_partition = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION}"],
        poke_interval=5,
    )
    # [END howto_sensor_named_hive_partition_async]

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
        >> create_hive_cli_airflow_connection
        >> create_hive_metastore_airflow_connection
        >> load_to_hive
        >> hive_sensor
        >> wait_for_partition
        >> remove_cluster
    )
