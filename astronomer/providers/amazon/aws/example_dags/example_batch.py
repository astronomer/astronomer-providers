"""Example DAG for the  AWS Batch Operator Async"""
import logging
import os
import time
from datetime import datetime
from json import loads
from os import environ

from airflow import DAG, AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from astronomer.providers.amazon.aws.operators.batch import BatchOperatorAsync
from astronomer.providers.amazon.aws.sensors.batch import BatchSensorAsync

AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "xxxxxxx")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "xxxxxxxx")

# The inputs below are required for the submit batch example DAG.
JOB_NAME = environ.get("BATCH_JOB_NAME", "test_airflow_job")
JOB_DEFINITION = environ.get("BATCH_JOB_DEFINITION", "providers_team_job_defn:1")
JOB_QUEUE = environ.get("BATCH_JOB_QUEUE", "providers_team_job_queue")
JOB_OVERRIDES = loads(environ.get("BATCH_JOB_OVERRIDES", "{}"))
JOB_COMPUTE_ENV = environ.get("BATCH_JOB_COMPUTE_ENV", "providers_team_compute_env")

COMPUTE_ENV_SPEC = {
    "type": "EC2",
    "allocationStrategy": "BEST_FIT",
    "minvCpus": 0,
    "maxvCpus": 2,
    "desiredvCpus": 2,
    "instanceTypes": [
        "m4.large",
    ],
    "subnets": ["subnet-055632983fc555b3d", "subnet-089dad9c602290749", "subnet-0c7eeba918f1086db"],
    "securityGroupIds": [
        "sg-0029c26643ae706e4",
    ],
    "instanceRole": "arn:aws:iam::396807896376:instance-profile/ecsInstanceRole",
    "ec2Configuration": [
        {
            "imageType": "ECS_AL2",
        },
    ],
}


def create_batch_compute_environment_func() -> None:
    """Create Batch compute environment"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")
    try:
        client.create_compute_environment(
            computeEnvironmentName=JOB_COMPUTE_ENV,
            type="MANAGED",
            state="ENABLED",
            computeResources=COMPUTE_ENV_SPEC,
            serviceRole="arn:aws:iam::396807896376:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch",
        )

        while get_compute_environment_status() != "VALID":
            logging.info("Waiting for compute environment to be VALID. Sleeping for 30 seconds.")
            time.sleep(30)

    except ClientError as error:
        logging.exception("Error while creating Batch compute environment")
        raise error


def get_compute_environment_status() -> str:
    """Get the status of aws batch compute environment"""
    import boto3

    client = boto3.client("batch")

    response = client.describe_compute_environments(
        computeEnvironments=[
            JOB_COMPUTE_ENV,
        ]
    )
    logging.info("%s", response)
    status = response.get("computeEnvironments")[0]
    final_status: str = status.get("status")
    return final_status


def create_job_queue_func() -> None:
    """Create Batch Job Queue"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")
    try:
        client.create_job_queue(
            jobQueueName=JOB_QUEUE,
            state="ENABLED",
            priority=1,
            computeEnvironmentOrder=[
                {"order": 1, "computeEnvironment": JOB_COMPUTE_ENV},
            ],
        )

        while get_job_queue_status() != "VALID":
            logging.info("Waiting for job queue to be VALID. Sleeping for 30 seconds.")
            time.sleep(30)
    except ClientError as error:
        logging.exception("Error while creating Batch Job queue ")
        raise error


def get_job_queue_status() -> str:
    """Get the status of aws batch job queue"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")

    try:
        response = client.describe_job_queues(
            jobQueues=[
                JOB_QUEUE,
            ]
        )
    except ClientError:
        response = {}

    logging.info("%s", response)
    if response.get("jobQueues"):
        status = response.get("jobQueues")[0]
        final_status: str = status.get("status")
        return final_status
    else:
        return "DELETED"


def list_jobs_func() -> str:
    """
    Gets the list of AWS batch jobs for the given job name.
    Ideally BatchOperatorAsync should push job ID to XCOM, but both the sync & async version
    operators don't have it in their implementation and thus we do not have the job ID that is
    needed for the BatchSensorAsync. Hence, we get the list of jobs by the job name and
    then extract job ID from the response.
    """
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")

    try:
        response = client.list_jobs(jobQueue=JOB_QUEUE, filters=[{"name": "JOB_NAME", "values": [JOB_NAME]}])
    except ClientError:
        response = {}

    logging.info("%s", response)
    if response.get("jobSummaryList"):
        # JobSummaryList returns list of jobs (it may contain duplicates as
        # AWS Batch allows submission of jobs with the same name) and sorted by createdAt
        res = response.get("jobSummaryList")[0]
        job_id: str = res.get("jobId")
    else:
        raise AirflowException("No jobs found")
    return job_id


def disable_compute_environment_func() -> None:
    """Disable Batch Compute Environment Job Definition"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")
    try:
        client.update_compute_environment(computeEnvironment=JOB_COMPUTE_ENV, state="DISABLED")

        while get_compute_environment_status() != "VALID":
            logging.info("Waiting for compute environment to be VALID. Sleeping for 30 seconds.")
            time.sleep(30)

    except ClientError as error:
        logging.exception("Error while disabling Batch Compute Environment")
        raise error


def delete_compute_environment_func() -> None:
    """Delete Batch Compute Environment Job Definition"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")
    try:
        client.delete_compute_environment(
            computeEnvironment=JOB_COMPUTE_ENV,
        )

    except ClientError as error:
        logging.exception("Error while deleting Batch Compute Environment")
        raise error


def disable_job_queue_func() -> None:
    """Disable Job Queue Function"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")
    try:
        client.update_job_queue(jobQueue=JOB_QUEUE, state="DISABLED")
        while get_job_queue_status() != "VALID":
            logging.info("Waiting for job queue to be Disabled. Sleeping for 30 seconds.")
            time.sleep(30)
    except ClientError as error:
        logging.exception("Error while disabling Batch Compute Environment")
        raise error


def delete_job_queue_func() -> None:
    """Delete Batch Job Queue"""
    import boto3
    from botocore.exceptions import ClientError

    client = boto3.client("batch")
    try:
        client.delete_job_queue(
            jobQueue=JOB_QUEUE,
        )
        while get_job_queue_status() == "DELETING":
            logging.info("Waiting for job queue to be DELETED. Sleeping for 30 seconds.")
            time.sleep(30)

    except ClientError as error:
        logging.exception("Error while deleting Batch Job Queue")
        raise error


with DAG(
    dag_id="example_async_batch",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["example", "async", "AWS"],
    catchup=False,
) as submit_dag:
    # Task steps for DAG to be self-sufficient
    setup_aws_config = BashOperator(
        task_id="setup_aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    # Task to create Batch compute environment
    create_batch_compute_environment = PythonOperator(
        task_id="create_batch_compute_environment",
        python_callable=create_batch_compute_environment_func,
    )

    # Task to create Batch Job Queue
    create_job_queue = PythonOperator(
        task_id="create_job_queue",
        python_callable=create_job_queue_func,
    )

    # [START howto_operator_batch_async]
    submit_batch_job = BatchOperatorAsync(
        task_id="submit_batch_job",
        job_name=JOB_NAME,
        job_queue=JOB_QUEUE,
        job_definition=JOB_DEFINITION,
        overrides=JOB_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_DEFAULT_REGION,
    )
    # [END howto_operator_batch_async]

    # Task to List jobs in AWS batch
    list_jobs = PythonOperator(
        task_id="list_jobs",
        python_callable=list_jobs_func,
    )
    # [START howto_sensor_batch_async]
    batch_job_sensor = BatchSensorAsync(
        task_id="sense_job",
        job_id="{{ task_instance.xcom_pull(task_ids='list_jobs', dag_id='example_async_batch', key='return_value') }}",
        aws_conn_id=AWS_CONN_ID,
        region_name=AWS_DEFAULT_REGION,
    )
    # [END howto_sensor_batch_async]

    disable_compute_environment = PythonOperator(
        task_id="disable_compute_environment",
        python_callable=disable_compute_environment_func,
        trigger_rule="all_done",
    )

    disable_job_queue = PythonOperator(
        task_id="disable_job_queue", python_callable=disable_job_queue_func, trigger_rule="all_done"
    )

    delete_job_queue = PythonOperator(
        task_id="delete_job_queue", python_callable=delete_job_queue_func, trigger_rule="all_done"
    )

    delete_compute_environment = PythonOperator(
        task_id="delete_compute_environment",
        python_callable=delete_compute_environment_func,
        trigger_rule="all_done",
    )

    end = DummyOperator(task_id="end")

    (
        setup_aws_config
        >> create_batch_compute_environment
        >> create_job_queue
        >> submit_batch_job
        >> list_jobs
        >> batch_job_sensor
        >> disable_compute_environment
        >> disable_job_queue
        >> delete_job_queue
        >> delete_compute_environment
    )

    [disable_compute_environment, disable_job_queue, delete_job_queue, delete_compute_environment] >> end
