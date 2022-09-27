import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.emr import EmrEksCreateClusterOperator

from astronomer.providers.amazon.aws.operators.emr import EmrContainerOperatorAsync
from astronomer.providers.amazon.aws.sensors.emr import EmrContainerSensorAsync

# [START howto_operator_emr_eks_env_variables]
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "xxxxxxx")
AWS_ACCOUNT_ID = os.getenv("AWS_ACCOUNT_ID", "xxxxxxxxxxxx")
AWS_CONN_ID = os.getenv("ASTRO_AWS_CONN_ID", "aws_default")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-2")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "xxxxxxxx")
CONTAINER_SUBMIT_JOB_POLICY = os.getenv(
    "CONTAINER_SUBMIT_JOB_POLICY", "test_emr_container_submit_jobs_policy"
)
DEBUGGING_MONITORING_POLICY = os.getenv("DEBUGGING_MONITORING_POLICY", "test_debugging_monitoring_policy")
EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "providers-team-eks-cluster")
EKS_NAMESPACE = os.getenv("EKS_NAMESPACE", "providers-team-eks-namespace")
EMR_INSTANCE_TYPE = os.getenv("EMR_INSTANCE_TYPE", "m5.xlarge")
JOB_EXECUTION_POLICY = os.getenv("JOB_EXECUTION_POLICY", "test_job_execution_policy")
JOB_EXECUTION_ROLE = os.getenv("JOB_EXECUTION_ROLE", "test_iam_job_execution_role")
JOB_ROLE_ARN = f"arn:aws:iam::{AWS_ACCOUNT_ID}:role/{JOB_EXECUTION_ROLE}"
MANAGE_VIRTUAL_CLUSTERS = os.getenv("MANAGE_VIRTUAL_CLUSTERS", "test_manage_virtual_clusters")
VIRTUAL_CLUSTER_NAME = os.getenv("EMR_VIRTUAL_CLUSTER_NAME", "providers-team-virtual-eks-cluster")
# [END howto_operator_emr_eks_env_variables]

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(os.getenv("DEFAULT_TASK_RETRIES", 2)),
    "retry_delay": timedelta(seconds=int(os.getenv("DEFAULT_RETRY_DELAY_SECONDS", 60))),
}


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
    dag_id="example_emr_eks_pi_job",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["example", "async", "emr"],
) as dag:
    # Task steps for DAG to be self-sufficient
    setup_aws_config = BashOperator(
        task_id="setup_aws_config",
        bash_command=f"aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}; "
        f"aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}; "
        f"aws configure set default.region {AWS_DEFAULT_REGION}; ",
    )

    # Task to create EMR on EKS cluster
    create_cluster_environment_variables = (
        f"AWS_ACCOUNT_ID={AWS_ACCOUNT_ID} "
        f"AWS_DEFAULT_REGION={AWS_DEFAULT_REGION} "
        f"CONTAINER_SUBMIT_JOB_POLICY={CONTAINER_SUBMIT_JOB_POLICY} "
        f"DEBUGGING_MONITORING_POLICY={DEBUGGING_MONITORING_POLICY} "
        f"EKS_CLUSTER_NAME={EKS_CLUSTER_NAME} "
        f"EKS_NAMESPACE={EKS_NAMESPACE} "
        f"EMR_INSTANCE_TYPE={EMR_INSTANCE_TYPE} "
        f"JOB_EXECUTION_POLICY={JOB_EXECUTION_POLICY} "
        f"JOB_EXECUTION_ROLE={JOB_EXECUTION_ROLE} "
        f"MANAGE_VIRTUAL_CLUSTERS={MANAGE_VIRTUAL_CLUSTERS}"
    )
    create_eks_cluster_kube_namespace_with_role = BashOperator(
        task_id="create_eks_cluster_kube_namespace_with_role",
        bash_command=f"{create_cluster_environment_variables} "
        f"sh $AIRFLOW_HOME/dags/example_create_eks_kube_namespace_with_role.sh ",
    )

    create_emr_virtual_cluster = EmrEksCreateClusterOperator(
        task_id="create_emr_eks_cluster",
        virtual_cluster_name=VIRTUAL_CLUSTER_NAME,
        eks_cluster_name=EKS_CLUSTER_NAME,
        eks_namespace=EKS_NAMESPACE,
    )

    VIRTUAL_CLUSTER_ID = create_emr_virtual_cluster.output

    # [START howto_operator_run_emr_container_job]
    run_emr_container_job = EmrContainerOperatorAsync(
        task_id="run_emr_container_job",
        virtual_cluster_id=str(VIRTUAL_CLUSTER_ID),
        execution_role_arn=JOB_ROLE_ARN,
        release_label="emr-6.2.0-latest",
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        name="pi.py",
    )
    # [END howto_operator_run_emr_container_job]

    # [START howto_sensor_emr_job_container_async]
    emr_job_container_sensor = EmrContainerSensorAsync(
        task_id="emr_job_container_sensor",
        job_id=str(run_emr_container_job.output),
        virtual_cluster_id=str(VIRTUAL_CLUSTER_ID),
        poll_interval=5,
        aws_conn_id=AWS_CONN_ID,
    )
    # [END howto_sensor_emr_job_container_async]

    # Delete EKS cluster, EMR containers, IAM role and detach role policies.r
    removal_environment_variables = (
        f"AWS_ACCOUNT_ID={AWS_ACCOUNT_ID} "
        f"CONTAINER_SUBMIT_JOB_POLICY={CONTAINER_SUBMIT_JOB_POLICY} "
        f"DEBUGGING_MONITORING_POLICY={DEBUGGING_MONITORING_POLICY} "
        f"EKS_CLUSTER_NAME={EKS_CLUSTER_NAME} "
        f"JOB_EXECUTION_POLICY={JOB_EXECUTION_POLICY} "
        f"JOB_EXECUTION_ROLE={JOB_EXECUTION_ROLE} "
        f"MANAGE_VIRTUAL_CLUSTERS={MANAGE_VIRTUAL_CLUSTERS} "
        f"VIRTUAL_CLUSTER_ID={VIRTUAL_CLUSTER_ID}"
    )
    remove_cluster_container_role_policy = BashOperator(
        task_id="remove_cluster_container_role_policy",
        bash_command=f"{removal_environment_variables} "
        f"sh $AIRFLOW_HOME/dags/example_delete_eks_cluster_and_role_policies.sh ",
        trigger_rule="all_done",
    )

    (
        setup_aws_config
        >> create_eks_cluster_kube_namespace_with_role
        >> create_emr_virtual_cluster
        >> run_emr_container_job
        >> emr_job_container_sensor
        >> remove_cluster_container_role_policy
    )
