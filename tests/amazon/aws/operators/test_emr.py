import os

from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator

from astronomer.providers.amazon.aws.operators.emr import EmrContainerOperatorAsync

VIRTUAL_CLUSTER_ID = os.getenv("VIRTUAL_CLUSTER_ID", "test-cluster")
JOB_ROLE_ARN = os.getenv("JOB_ROLE_ARN", "arn:aws:iam::012345678912:role/emr_eks_default_role")
JOB_ID = "jobid-12122"

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

EMR_OPERATOR = EmrContainerOperatorAsync(
    task_id="start_job",
    virtual_cluster_id=VIRTUAL_CLUSTER_ID,
    execution_role_arn=JOB_ROLE_ARN,
    release_label="emr-6.3.0-latest",
    job_driver=JOB_DRIVER_ARG,
    configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
    name="pi.py",
)


class TestEmrContainerOperatorAsync:
    def test_init(self):
        task = EmrContainerOperatorAsync(
            task_id="start_job",
            virtual_cluster_id=VIRTUAL_CLUSTER_ID,
            execution_role_arn=JOB_ROLE_ARN,
            release_label="emr-6.3.0-latest",
            job_driver=JOB_DRIVER_ARG,
            configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
            name="pi.py",
        )
        assert isinstance(task, EmrContainerOperator)
        assert task.deferrable is True
