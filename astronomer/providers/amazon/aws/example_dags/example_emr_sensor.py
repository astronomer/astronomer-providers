"""Example DAG for AWS EMR related operator and sensor"""

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule

from astronomer.providers.amazon.aws.sensors.emr import (
    EmrJobFlowSensorAsync,
    EmrStepSensorAsync,
)

JOB_FLOW_ROLE = os.getenv("EMR_JOB_FLOW_ROLE", "EMR_EC2_DefaultRole")
SERVICE_ROLE = os.getenv("EMR_SERVICE_ROLE", "EMR_DefaultRole")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

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
    "Name": "example_emr_sensor_cluster",
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
    "LogUri": "s3://example_emr_sensor_cluster/",
}

DEFAULT_ARGS = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


def check_dag_status(**kwargs: Any) -> None:
    """Raises an exception if any of the DAG's tasks failed and as a result marking the DAG failed."""
    for task_instance in kwargs["dag_run"].get_task_instances():
        if (
            task_instance.current_state() != State.SUCCESS
            and task_instance.task_id != kwargs["task_instance"].task_id
        ):
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")


with DAG(
    dag_id="example_emr_sensor",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    default_args=DEFAULT_ARGS,
    tags=["example", "async", "emr"],
    catchup=False,
) as dag:
    # For apache-airflow-providers-amazon < 4.1.0 you will also have to pass emr_conn_id param
    # [START howto_operator_emr_create_job_flow_steps_tasks]
    create_job_flow = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_emr_create_job_flow_steps_tasks]

    # [START howto_operator_emr_add_steps]
    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=create_job_flow.output,  # type: ignore[arg-type]
        steps=SPARK_STEPS,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_operator_emr_add_steps]

    # [START howto_sensor_emr_job_flow_async]
    job_flow_sensor = EmrJobFlowSensorAsync(
        task_id="job_flow_sensor", job_flow_id=create_job_flow.output, aws_conn_id=AWS_CONN_ID
    )
    # [END howto_sensor_emr_job_flow_async]

    # [START howto_sensor_emr_step_async]
    watch_step = EmrStepSensorAsync(
        task_id="watch_step",
        job_flow_id=create_job_flow.output,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_emr_step_async]

    # [START howto_operator_emr_terminate_job_flow]
    remove_job_flow = EmrTerminateJobFlowOperator(
        task_id="remove_job_flow",
        job_flow_id=create_job_flow.output,  # type: ignore[arg-type]
        aws_conn_id=AWS_CONN_ID,
        trigger_rule="all_done",
    )
    # [END howto_operator_emr_terminate_job_flow]

    dag_final_status = PythonOperator(
        task_id="dag_final_status",
        provide_context=True,
        python_callable=check_dag_status,
        trigger_rule=TriggerRule.ALL_DONE,  # Ensures this task runs even if upstream fails
        dag=dag,
        retries=0,
    )

    add_steps >> watch_step
    [job_flow_sensor, watch_step] >> remove_job_flow >> dag_final_status
