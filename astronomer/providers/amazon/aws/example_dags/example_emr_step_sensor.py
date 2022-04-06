"""Example DAG for AWS EMR related operator and sensor"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)

from astronomer.providers.amazon.aws.sensors.emr import EmrStepSensorAsync

JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
EMR_CONN_ID = os.getenv("ASTRO_EMR_CONN_ID", "emr_default")


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
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "JobFlowRole": JOB_FLOW_ROLE,
    "ServiceRole": SERVICE_ROLE,
}

DEFAULT_ARGS = {
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="example_emr_step_sensor",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    default_args=DEFAULT_ARGS,
    tags=["example", "async", "emr"],
    catchup=False,
) as dag:

    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        emr_conn_id=EMR_CONN_ID,
    )

    # [START howto_operator_emr_add_steps]
    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=cluster_creator.output,
        steps=SPARK_STEPS,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_emr_add_steps]

    # [START howto_sensor_emr_step_sensor_async]
    """
    Defer and poll until it reaches the target state
    The Default value of target state is COMPLETED
    For more detail see here
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step
    """
    step_checker = EmrStepSensorAsync(
        task_id="watch_step",
        job_flow_id=cluster_creator.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_emr_step_sensor_async]

    # [START howto_operator_emr_terminate_job_flow]
    cluster_remover = EmrTerminateJobFlowOperator(
        task_id="remove_cluster",
        job_flow_id=cluster_creator.output,
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    )
    # [END howto_operator_emr_terminate_job_flow]

    chain(
        step_adder,
        step_checker,
        cluster_remover,
    )
