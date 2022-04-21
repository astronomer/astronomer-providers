import os
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from astronomer.providers.amazon.aws.operators.emr import EmrContainerOperatorAsync
from astronomer.providers.amazon.aws.triggers.emr import EmrContainerOperatorTrigger

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


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def _emr_emr_container_operator_init():
    """Return an instance of EmrContainerOperatorAsync operator"""
    return EmrContainerOperatorAsync(
        task_id="start_job",
        virtual_cluster_id=VIRTUAL_CLUSTER_ID,
        execution_role_arn=JOB_ROLE_ARN,
        release_label="emr-6.3.0-latest",
        job_driver=JOB_DRIVER_ARG,
        configuration_overrides=CONFIGURATION_OVERRIDES_ARG,
        name="pi.py",
    )


@mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
def test_emr_container_operator_async(check_job_status):
    """Assert EmrContainerOperatorAsync defer"""
    check_job_status.return_value = JOB_ID
    with pytest.raises(TaskDeferred) as exc:
        _emr_emr_container_operator_init().execute(context)

    assert isinstance(
        exc.value.trigger, EmrContainerOperatorTrigger
    ), "Trigger is not a EmrContainerOperatorTrigger"


@mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
def test_emr_container_operator_execute_complete_success(check_job_status):
    """Assert execute_complete succeed"""
    check_job_status.return_value = JOB_ID
    assert (
        _emr_emr_container_operator_init().execute_complete(
            context=None, event={"status": "success", "message": "Job completed", "job_id": JOB_ID}
        )
        == JOB_ID
    )


@mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
def test_emr_container_operator_execute_complete_fail(check_job_status):
    """Assert execute_complete throw AirflowException"""
    check_job_status.return_value = JOB_ID
    with pytest.raises(AirflowException):
        _emr_emr_container_operator_init().execute_complete(
            context=None, event={"status": "error", "message": "test failure message"}
        )
