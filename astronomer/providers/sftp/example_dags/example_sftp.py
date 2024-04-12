import json
import logging
import os
import time
from datetime import timedelta
from typing import Any, List

from airflow import DAG, AirflowException, settings
from airflow.models import Connection, TaskInstance, Variable
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.trigger_rule import TriggerRule
from requests import get

from astronomer.providers.sftp.sensors.sftp import SFTPSensorAsync

SFTP_CONN_ID = os.getenv("ASTRO_SFTP_CONN_ID", "sftp_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
AWS_S3_CREDS = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "aws_access_key"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "aws_secret_key"),
    "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
}
AMI_ID = os.getenv("AMI_ID", "test")
PEM_FILENAME = os.getenv("PEM_FILENAME", "providers_team_keypair")
PRIVATE_KEY = Variable.get("providers_team_keypair")
INBOUND_SECURITY_GROUP = os.getenv("INBOUND_SECURITY_GROUP", "security-group")
SFTP_SSH_PORT = int(os.getenv("SFTP_SSH_PORT", 22))
SFTP_INSTANCE_TYPE = os.getenv("SFTP_INSTANCE_TYPE", "t2.micro")
BOTO_DUPLICATE_PERMISSION_ERROR = "InvalidPermission.Duplicate"
EC2_INSTANCE_ID_KEY = "ec2_instance_id"
INSTANCE_PUBLIC_DNS_NAME_KEY = "instance_public_dns_name"
INSTANCE_SECURITY_GROUP = "instance_response_master_security_group"


COMMAND_TO_CREATE_TABLE_DATA_FILE: List[str] = [
    "curl https://raw.githubusercontent.com/astronomer/astronomer-providers/\
main/astronomer/providers/apache/hive/example_dags/zipcodes.csv \
 >> zipcodes.csv",
    "mv zipcodes.csv /home/ubuntu/",
]

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def create_sftp_airflow_connection(task_instance: Any) -> None:
    """
    Checks if airflow connection exists, if yes then deletes it.
    Then, create a new sftp_default connection.
    """
    conn = Connection(
        conn_id="sftp_default",
        conn_type="sftp",
        host=task_instance.xcom_pull(key=INSTANCE_PUBLIC_DNS_NAME_KEY, task_ids=["create_ec2_instance"])[0],
        login="ubuntu",
        port=SFTP_SSH_PORT,
        extra=json.dumps(
            {
                "private_key": PRIVATE_KEY,
                "no_host_key_check": "true",
                "known_hosts": "none",
            }
        ),
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
    logging.info("Connection sftp_default is created")


def create_instance_with_security_group() -> None:
    """Create ec2 instance"""
    import boto3

    ec2 = boto3.resource("ec2", **AWS_S3_CREDS)
    instance = ec2.create_instances(
        ImageId=AMI_ID,
        MinCount=1,
        MaxCount=1,
        InstanceType=SFTP_INSTANCE_TYPE,
        KeyName=PEM_FILENAME,
        SecurityGroups=[INBOUND_SECURITY_GROUP],
    )
    instance_id = instance[0].id
    ti = get_current_context()["ti"]
    ti.xcom_push(key=EC2_INSTANCE_ID_KEY, value=instance_id)
    while get_instances_status(instance_id) != "running":
        logging.info("Waiting for Instance to be available in running state. Sleeping for 30 seconds.")
        time.sleep(30)


def get_instances_status(instance_id: str) -> str:
    """Get the instance status by id"""
    import boto3

    client = boto3.client("ec2", **AWS_S3_CREDS)
    response = client.describe_instances(
        InstanceIds=[instance_id],
    )
    instance_details = response["Reservations"][0]["Instances"][0]
    instance_state: str = instance_details["State"]["Name"]
    if instance_state == "running":
        ti = get_current_context()["ti"]
        ti.xcom_push(key=INSTANCE_SECURITY_GROUP, value=instance_details["SecurityGroups"][0]["GroupId"])
        ti.xcom_push(key=INSTANCE_PUBLIC_DNS_NAME_KEY, value=instance_details["PublicDnsName"])
    return instance_state


def add_inbound_rule_for_security_group(task_instance: "TaskInstance") -> None:
    """
    Sets the inbound rule for the aws security group, based on
    current ip address of the system.
    """
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("ec2", **AWS_S3_CREDS)
    current_docker_ip = get("https://api.ipify.org").text

    # Allow SSH traffic on port 22 and copy file to ec2 instance.
    try:
        client.authorize_security_group_ingress(
            GroupId=task_instance.xcom_pull(key=INSTANCE_SECURITY_GROUP, task_ids=["create_ec2_instance"])[0],
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


def revoke_inbound_rules(task_instance: TaskInstance) -> None:
    """Remove an ingress rule from security group"""
    import boto3

    current_docker_ip = get("https://api.ipify.org").text
    ip_range = str(current_docker_ip) + "/32"
    logging.info("Trying to revoke ingress ip address is: %s", str(ip_range))
    client = boto3.client("ec2", **AWS_S3_CREDS)
    response = client.revoke_security_group_ingress(
        CidrIp=ip_range,
        FromPort=22,
        ToPort=22,
        GroupId=task_instance.xcom_pull(key=INSTANCE_SECURITY_GROUP, task_ids=["create_ec2_instance"])[0],
        IpProtocol="tcp",
    )
    logging.info("%s", response)


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
    instance_public_dns_name = task_instance.xcom_pull(
        key=INSTANCE_PUBLIC_DNS_NAME_KEY, task_ids=["create_ec2_instance"]
    )[0]
    client.connect(hostname=instance_public_dns_name, username=kwargs["username"], pkey=key)

    # Execute a command(cmd) after connecting/ssh to an instance
    for command in kwargs["command"]:
        stdin, stdout, stderr = client.exec_command(command)
        stdout.read()

    # close the client connection once the job is done
    client.close()


def terminate_instance(task_instance: "TaskInstance") -> None:
    """Terminate ec2 instance by instance id"""
    import boto3

    ec2 = boto3.client("ec2", **AWS_S3_CREDS)
    ec2_instance_id_xcom = task_instance.xcom_pull(key=EC2_INSTANCE_ID_KEY, task_ids=["create_ec2_instance"])[
        0
    ]
    ec2.terminate_instances(
        InstanceIds=[
            ec2_instance_id_xcom,
        ],
    )


def check_dag_status(**kwargs: Any) -> None:
    """Raises an exception if any of the DAG's tasks failed and as a result marking the DAG failed."""
    for task_instance in kwargs["dag_run"].get_task_instances():
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")


with DAG(
    dag_id="example_async_sftp_sensor",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "sftp"],
) as dag:
    create_ec2_instance = PythonOperator(
        task_id="create_ec2_instance", python_callable=create_instance_with_security_group
    )

    get_and_add_ip_address_for_inbound_rules = PythonOperator(
        task_id="get_and_add_ip_address_for_inbound_rules",
        python_callable=add_inbound_rule_for_security_group,
    )

    ssh_and_copy_file = PythonOperator(
        task_id="ssh_and_copy_file",
        python_callable=ssh_and_run_command,
        op_kwargs={
            "path_to_pem_file": f"/tmp/{PEM_FILENAME}.pem",
            "username": "ubuntu",
            "command": COMMAND_TO_CREATE_TABLE_DATA_FILE,
        },
    )

    create_sftp_default_airflow_connection = PythonOperator(
        task_id="create_sftp_default_airflow_connection",
        python_callable=create_sftp_airflow_connection,
    )

    # [START howto_sensor_sftp_async]
    async_sftp_sensor = SFTPSensorAsync(
        task_id="async_sftp_sensor",
        sftp_conn_id=SFTP_CONN_ID,
        path="/home/ubuntu/",
        file_pattern="*.csv",
        poke_interval=5,
    )
    # [END howto_sensor_sftp_async]

    # [START howto_sensor_sftp_async]
    # without file pattern
    async_sftp_sensor_without_pattern = SFTPSensorAsync(
        task_id="async_sftp_sensor_without_pattern",
        sftp_conn_id=SFTP_CONN_ID,
        path="/home/ubuntu/zipcodes.csv",
        poke_interval=5,
    )
    # [END howto_sensor_sftp_async]

    terminate_ec2_instance = PythonOperator(
        task_id="terminate_instance", trigger_rule=TriggerRule.ALL_DONE, python_callable=terminate_instance
    )

    revoke_inbound_rule = PythonOperator(
        task_id="revoke_inbound_rules",
        trigger_rule=TriggerRule.ALL_DONE,
        python_callable=revoke_inbound_rules,
    )

    dag_final_status = PythonOperator(
        task_id="dag_final_status",
        provide_context=True,
        python_callable=check_dag_status,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        dag=dag,
        retries=0,
    )

    (
        create_ec2_instance
        >> get_and_add_ip_address_for_inbound_rules
        >> ssh_and_copy_file
        >> create_sftp_default_airflow_connection
        >> [async_sftp_sensor, async_sftp_sensor_without_pattern]
        >> terminate_ec2_instance
        >> revoke_inbound_rule
        >> dag_final_status
    )
