import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)

from astronomer.providers.amazon.aws.sensors.emr import EmrJobFlowSensorAsync

JOB_FLOW_ROLE = os.environ.get("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.environ.get("EMR_SERVICE_ROLE", "EMR_DefaultRole")
AWS_CONN_ID = os.environ.get("ASTRO_AWS_CONN_ID", "s3_default")

default_args = {
    "execution_timeout": timedelta(minutes=30),
}

SPARK_STEPS = [
    {
        "Name": "calculate_pi",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    "Name": "PiCalc",
    "ReleaseLabel": "emr-5.29.0",
    "Applications": [{"Name": "Spark"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Primary node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            }
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "Steps": SPARK_STEPS,
    "JobFlowRole": JOB_FLOW_ROLE,
    "ServiceRole": SERVICE_ROLE,
}
# [END howto_operator_emr_automatic_steps_config]

with DAG(
    dag_id="example_emr_job_flow_sensor",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "emr"],
) as dag:

    # [START howto_operator_emr_create_job_flow_steps_tasks]
    job_flow_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )
    # [END howto_operator_emr_create_job_flow_steps_tasks]

    # [START howto_sensor_emr_job_flow_sensor_async]
    job_flow_sensor = EmrJobFlowSensorAsync(task_id="job_flow_sensor", job_flow_id=job_flow_creator.output)
    # [END howto_sensor_emr_job_flow_sensor_async]

    # [START howto_operator_emr_terminate_job_flow]
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=job_flow_creator.output,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_emr_terminate_job_flow]
    chain(job_flow_creator, job_flow_sensor, cluster_remover)
