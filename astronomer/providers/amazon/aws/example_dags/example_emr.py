import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator

from astronomer.providers.amazon.aws.sensors.emr import EmrContainerSensorAsync

# [START howto_operator_emr_eks_env_variables]
VIRTUAL_CLUSTER_ID = os.getenv("VIRTUAL_CLUSTER_ID", "astro-emr-cluster")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::012345678912:role/emr_eks_default_role")

default_args = {
    "execution_timeout": timedelta(minutes=30),
}
# [END howto_operator_emr_eks_env_variables]


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
    tags=["example", "async", "emr_container"],
) as dag:

    # An example of how to get the cluster id and arn from an Airflow connection
    # VIRTUAL_CLUSTER_ID = '{{ conn.emr_eks.extra_dejson["virtual_cluster_id"] }}'
    # JOB_ROLE_ARN = '{{ conn.emr_eks.extra_dejson["job_role_arn"] }}'

    # [START howto_operator_emr_eks_jobrun]
    job_starter = EmrContainerOperator(
        task_id="start_job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        execution_role_arn=JOB_ROLE_ARN,
        release_label="emr-5.35.0",
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        name="pi.py",
        max_tries=0,
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

    job_starter >> job_container_sensor
