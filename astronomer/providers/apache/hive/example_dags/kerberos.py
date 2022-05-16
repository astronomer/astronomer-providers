import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, List

from airflow import DAG, settings
from airflow.models import Connection, Variable
from airflow.operators.bash import BashOperator
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

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))
EMR_CLUSTER_SECURITY_CONFIG = os.getenv(
    "EMR_CLUSTER_SECURITY_CONFIG", "provider-team-kerberos-security-config"
)
EMR_CLUSTER_NAME = os.getenv("EMR_CLUSTER_NAME", "example_hive_sensor_cluster")
JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
PEM_FILENAME = os.getenv("PEM_FILENAME", "providers_team_keypair")
HIVE_OPERATOR_INGRESS_PORT = int(os.getenv("HIVE_OPERATOR_INGRESS_PORT", 10000))
HIVE_SCHEMA = os.getenv("HIVE_SCHEMA", "default")
HIVE_TABLE = os.getenv("HIVE_TABLE", "zipcode")
HIVE_PARTITION = os.getenv("HIVE_PARTITION", "state='FL'")
PRIVATE_KEY = Variable.get("providers_team_keypair")

COMMAND_TO_CREATE_TABLE_DATA_FILE: List[str] = [
    "curl https://raw.githubusercontent.com/astronomer/astronomer-providers/\
main/astronomer/providers/apache/hive/example_dags/zipcodes.csv \
 >> zipcodes.csv",
    "hdfs dfs -put zipcodes.csv /user/root",
]

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
}

AWS_CREDENTIAL = {
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "**********"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "***********"),
    "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-2"),
}

JOB_FLOW_OVERRIDES = {
    "Name": EMR_CLUSTER_NAME,
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
    "SecurityConfiguration": EMR_CLUSTER_SECURITY_CONFIG,
    "KerberosAttributes": {"KdcAdminPassword": "a*z-0*9", "Realm": "EC2.INTERNAL"},
}


def create_security_configuration() -> None:
    """Create security config with cluster dedicated provider having ticket lifetime 240 hrs"""
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    SecurityConfiguration = {
        "AuthenticationConfiguration": {
            "KerberosConfiguration": {
                "Provider": "ClusterDedicatedKdc",
                "ClusterDedicatedKdcConfiguration": {"TicketLifetimeInHours": 240},
            }
        }
    }

    client.create_security_configuration(
        Name=EMR_CLUSTER_SECURITY_CONFIG, SecurityConfiguration=json.dumps(SecurityConfiguration)
    )


def get_emr_cluster_status(cluster_id: str) -> str:
    """Get EMR cluster status"""
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    response = client.describe_cluster(ClusterId=cluster_id)
    state: str = response["Cluster"]["Status"]["State"]
    return state


def wait_for_cluster(ti: Any) -> None:
    """Wait for EMR cluster to reach RUNNING or WAITING state"""
    while True:
        status: str = get_emr_cluster_status(
            str(ti.xcom_pull(key="return_value", task_ids=["create_cluster"])[0])
        )
        if status in ["RUNNING", "WAITING"]:
            return
        else:
            logging.info("Cluster status is %s. Sleeping for 60 seconds", status)
            time.sleep(60)


def cache_cluster_details(ti: Any) -> None:
    """
    Store EMR cluster MasterPublicDnsName, EmrManagedMasterSecurityGroup and
    PrivateIpAddress in the XCOM.
    """
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    cluster_id = str(ti.xcom_pull(key="return_value", task_ids=["create_cluster"])[0])
    response = client.describe_cluster(ClusterId=cluster_id)
    logging.info("Cluster configuration : %s", str(response))
    ti.xcom_push(key="master_public_dns", value=response["Cluster"]["MasterPublicDnsName"])
    ti.xcom_push(
        key="cluster_response_master_security_group",
        value=response["Cluster"]["Ec2InstanceAttributes"]["EmrManagedMasterSecurityGroup"],
    )
    response = client.list_instances(ClusterId=cluster_id)
    ti.xcom_push(
        key="master_private_ip",
        value=response["Instances"][len(response["Instances"]) - 1]["PrivateIpAddress"],
    )


def add_inbound_rule(ti: Any) -> None:
    """
    Add inbound rule in security group

        1. Tcp, port 22 for ssh
        2. Tcp, port 88 for Kerberos
        3. Udp, port 88 for Kerberos
        4. Tcp, port 10000 for Hive
    """
    import boto3
    from botocore.exceptions import ClientError

    container_ip = get("https://api.ipify.org").text
    logging.info("The container ip address is: %s", str(container_ip))

    inbound_rules = [
        {
            "IpProtocol": "tcp",
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 88,
            "ToPort": 88,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
        {
            "IpProtocol": "udp",
            "FromPort": 88,
            "ToPort": 88,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 10000,
            "ToPort": 10000,
            "IpRanges": [{"CidrIp": str(container_ip) + "/32"}],
        },
    ]

    client = boto3.client("ec2", **AWS_CREDENTIAL)
    for _inbound_rule in inbound_rules:
        try:
            client.authorize_security_group_ingress(
                GroupId=ti.xcom_pull(
                    key="cluster_response_master_security_group", task_ids=["cache_cluster_details"]
                )[0],
                IpPermissions=[_inbound_rule],
            )
            logging.info("Added inbound rule %s", _inbound_rule)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code", "") == "InvalidPermission.Duplicate":
                logging.info("Inbound rule %s exists.", _inbound_rule)
                logging.error(
                    "Ingress for port %s already authorized. Error message is: %s",
                    _inbound_rule.get("FromPort"),
                    error.response["Error"]["Message"],
                )
            else:
                logging.info("Failed to add inbound rule %s", _inbound_rule)
                raise error


def create_key_pair() -> None:
    """
    Load the private_key from airflow variable and creates a pem_file
    at /tmp/.
    """
    # remove the file if it exists
    if os.path.exists(f"/tmp/{PEM_FILENAME}.pem"):
        os.remove(f"/tmp/{PEM_FILENAME}.pem")

    # read the content for pem file from Variable set on Airflow UI.
    with open(f"/tmp/{PEM_FILENAME}.pem", "w+") as fh:
        fh.write(PRIVATE_KEY)

    # write private key to file with 400 permissions
    os.chmod(f"/tmp/{PEM_FILENAME}.pem", 0o400)


def gen_kerberos_credential(ti: Any) -> None:
    """Generate keytab file on EMR master node"""
    import paramiko

    key = paramiko.RSAKey.from_private_key_file(f"/tmp/{PEM_FILENAME}.pem")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    master_public_dns = ti.xcom_pull(key="master_public_dns", task_ids=["cache_cluster_details"])[0]
    logging.info("Connecting to %s", master_public_dns)
    client.connect(hostname=master_public_dns, username="hadoop", pkey=key)

    # Execute a command(cmd) after connecting/ssh to an instance
    commands = [
        "sudo kadmin.local -q 'addprinc -randkey airflow'",
        "sudo kadmin.local -q 'xst -norandkey -k airflow.keytab airflow'",
        "sudo kadmin.local -q 'q'",
        "sudo chmod 777 airflow.keytab",
    ]
    for command in commands:
        logging.info("Running %s", command)
        stdin, stdout, stderr = client.exec_command(command)
        stdout.read()

    client.close()


def get_kerberos_credential(ti: Any) -> None:
    """Copy kerberos configuration and keytab files from EMR master node to airflow container"""
    import subprocess

    master_public_dns = ti.xcom_pull(key="master_public_dns", task_ids=["cache_cluster_details"])[0]

    copy_krb5 = (
        f'scp  -o "StrictHostKeyChecking no" '
        f"-i /tmp/{PEM_FILENAME}.pem hadoop@{master_public_dns}:/etc/krb5.conf dags/"
    )
    subprocess.run(copy_krb5, shell=True, check=True)
    copy_keytab = (
        f'scp  -o "StrictHostKeyChecking no" '
        f"-i /tmp/{PEM_FILENAME}.pem hadoop@{master_public_dns}:airflow.keytab dags/"
    )
    subprocess.run(copy_keytab, shell=True, check=True)


def create_hive_cli_conn(ti: Any) -> None:
    """
    Create Hive CLI connection to run hive sync version of operator
    if connection already exist then delete it. Create a new connection.
    """
    master_private_ip = ti.xcom_pull(key="master_private_ip", task_ids=["cache_cluster_details"])[0]
    conn = Connection(
        conn_id="hive_cli_conn1",
        conn_type="hive_cli",
        host=ti.xcom_pull(key="master_public_dns", task_ids=["cache_cluster_details"])[0],
        login="hive",
        password="password",
        port=HIVE_OPERATOR_INGRESS_PORT,
        schema="default",
        extra={
            "use_beeline": True,
            "principal": f"hive/{master_private_ip}.us-east-2.compute.internal@EC2_INTERNAL",
        },
    )

    session = settings.Session()
    connection = session.query(Connection).filter_by(conn_id=conn.conn_id).one_or_none()
    if connection is not None:
        logging.info("Connection exist! Deleting connection %s.", str(conn.conn_id))
        session.delete(connection)
        session.commit()
        logging.info("Deleted old connection %s.", str(conn.conn_id))

    session.add(conn)
    session.commit()
    logging.info("Connection metastore_default is created")


def create_kerberos_hive_cli_conn(ti: Any) -> None:
    """
    Create Hive CLI connection to run async version tasks.
    if connection already exist then delete it. Create a new connection.
    """
    conn = Connection(
        conn_id="hive_cli2",
        conn_type="hive_cli",
        host=ti.xcom_pull(key="master_public_dns", task_ids=["cache_cluster_details"])[0],
        login="hive",
        password="password",
        port=HIVE_OPERATOR_INGRESS_PORT,
        schema="default",
        extra={"authMechanism": "GSSAPI", "kerberos_service_name": "hive"},
    )

    session = settings.Session()
    connection = session.query(Connection).filter_by(conn_id=conn.conn_id).one_or_none()
    if connection is not None:
        logging.info("Connection exist! Deleting connection %s.", str(conn.conn_id))
        session.delete(connection)
        session.commit()
        logging.info("Deleted old connection %s.", str(conn.conn_id))

    session.add(conn)
    session.commit()
    logging.info("Connection metastore_default is created")


def copy_data_to_hdfs(ti: Any, **kwargs: Any) -> None:
    """
    SSH into the machine and execute the bash script from the list
    of commands.
    """
    import paramiko

    key = paramiko.RSAKey.from_private_key_file(kwargs["path_to_pem_file"])
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Connect/ssh to an instance
    master_public_dns = ti.xcom_pull(key="master_public_dns", task_ids=["cache_cluster_details"])[0]
    client.connect(hostname=master_public_dns, username=kwargs["username"], pkey=key)

    # Execute a command(cmd) after connecting/ssh to an instance
    for command in kwargs["command"]:
        stdin, stdout, stderr = client.exec_command(command)
        stdout.read()

    # close the client connection once the job is done
    client.close()


def delete_security_configuration() -> None:
    """Delete security configuration"""
    import boto3

    client = boto3.client("emr", **AWS_CREDENTIAL)
    client.delete_security_configuration(Name=EMR_CLUSTER_SECURITY_CONFIG)


with DAG(
    dag_id="kerberos_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["hive", "kerberos"],
) as dag:
    create_security_config = PythonOperator(
        task_id="create_security_config",
        python_callable=create_security_configuration,
    )

    create_cluster_op = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    wait_for_cluster_op = PythonOperator(task_id="wait_for_cluster", python_callable=wait_for_cluster)

    cache_cluster_details_op = PythonOperator(
        task_id="cache_cluster_details", python_callable=cache_cluster_details
    )

    inbound_rule = PythonOperator(
        task_id="add_inbound_rule",
        python_callable=add_inbound_rule,
    )

    create_key_pair_op = PythonOperator(task_id="create_key_pair", python_callable=create_key_pair)

    gen_kerberos_credential_op = PythonOperator(
        task_id="gen_kerberos_credential", python_callable=gen_kerberos_credential
    )

    get_kerberos_credential_op = PythonOperator(
        task_id="get_kerberos_credential", python_callable=get_kerberos_credential
    )

    # 1. Add a mapping from Hive master node public ip address to private DNS name in /etc/hosts file
    # 2. Change KDC and server value from cluster private DNS name to public DNS name in dags/krb5.conf
    # 3. copy dags/krb5.conf in etc/ folder
    # 4. run ``KRB5_TRACE=/dev/stdout airflow kerberos``
    # 5. Step 1-4 need tobe done on both worker and trigger container
    wait = PythonOperator(
        task_id="wait",
        python_callable=lambda: time.sleep(240),
    )

    kerberos_init = BashOperator(
        task_id="run_kerberos_init", bash_command="KRB5_TRACE=/dev/stdout airflow kerberos &"
    )

    create_hive_cli_conn_op = PythonOperator(
        task_id="create_hive_cli_conn", python_callable=create_hive_cli_conn
    )

    create_kerberos_hive_cli_conn_op = PythonOperator(
        task_id="create_kerberos_hive_cli_conn", python_callable=create_kerberos_hive_cli_conn
    )

    copy_data_to_hdfs_op = PythonOperator(
        task_id="copy_data_in_hdfs",
        python_callable=copy_data_to_hdfs,
        op_kwargs={
            "path_to_pem_file": f"/tmp/{PEM_FILENAME}.pem",
            "username": "hadoop",
            "command": COMMAND_TO_CREATE_TABLE_DATA_FILE,
        },
    )

    load_to_hive = HiveOperator(
        task_id="hive_query",
        hql=(
            "CREATE TABLE zipcode(RecordNumber int,Country string,City string,Zipcode int) "
            "PARTITIONED BY(state string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
            "CREATE TABLE zipcodes_tmp(RecordNumber int,Country string,City string,Zipcode int,state string)"
            "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';"
            "LOAD DATA INPATH '/user/root/zipcodes.csv' INTO TABLE zipcodes_tmp;"
            "set hive.exec.dynamic.partition.mode=nonstrict;"
            "INSERT into zipcode PARTITION(state) SELECT * from  zipcodes_tmp;"
        ),
        hive_cli_conn_id="hive_cli_conn1",
    )

    hive_sensor = HivePartitionSensorAsync(
        task_id="hive_partition_check",
        table=HIVE_TABLE,
        partition=HIVE_PARTITION,
        poke_interval=5,
        metastore_conn_id="hive_cli2",
    )

    wait_for_partition_op = NamedHivePartitionSensorAsync(
        task_id="wait_for_partition",
        partition_names=[f"{HIVE_SCHEMA}.{HIVE_TABLE}/{HIVE_PARTITION}"],
        poke_interval=5,
        metastore_conn_id="hive_cli2",
    )

    remove_cluster_op = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=create_cluster_op.output,
        trigger_rule="all_done",
    )

    delete_security_config = PythonOperator(
        task_id="delete_security_config",
        python_callable=delete_security_configuration,
        trigger_rule="all_done",
    )

    (
        create_security_config
        >> create_cluster_op
        >> wait_for_cluster_op
        >> cache_cluster_details_op
        >> inbound_rule
        >> create_key_pair_op
        >> gen_kerberos_credential_op
        >> get_kerberos_credential_op
        >> create_hive_cli_conn_op
        >> create_kerberos_hive_cli_conn_op
        >> wait
        >> kerberos_init
        >> copy_data_to_hdfs_op
        >> load_to_hive
        >> hive_sensor
        >> wait_for_partition_op
        >> remove_cluster_op
        >> delete_security_config
    )
