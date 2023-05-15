import os
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook

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
    @pytest.mark.parametrize("status", EmrContainerHook.SUCCESS_STATES)
    @mock.patch("astronomer.providers.amazon.aws.operators.emr.EmrContainerOperatorAsync.defer")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.check_query_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
    def test_emr_container_operator_async_succeeded_before_defer(
        self, check_job_status, check_query_status, defer, status, context
    ):
        check_job_status.return_value = JOB_ID
        check_query_status.return_value = status
        assert EMR_OPERATOR.execute(context) == JOB_ID

        assert not defer.called

    @pytest.mark.parametrize("status", EmrContainerHook.FAILURE_STATES)
    @mock.patch("astronomer.providers.amazon.aws.operators.emr.EmrContainerOperatorAsync.defer")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.get_job_failure_reason")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.check_query_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
    def test_emr_container_operator_async_terminal_before_defer(
        self, check_job_status, check_query_status, get_job_failure_reason, defer, status, context
    ):
        check_job_status.return_value = JOB_ID
        check_query_status.return_value = status

        with pytest.raises(AirflowException):
            EMR_OPERATOR.execute(context)

        assert not defer.called

    @pytest.mark.parametrize("status", EmrContainerHook.INTERMEDIATE_STATES)
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.check_query_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
    def test_emr_container_operator_async(self, check_job_status, check_query_status, status, context):
        check_job_status.return_value = JOB_ID
        check_query_status.return_value = status
        with pytest.raises(TaskDeferred) as exc:
            EMR_OPERATOR.execute(context)

        assert isinstance(
            exc.value.trigger, EmrContainerOperatorTrigger
        ), "Trigger is not a EmrContainerOperatorTrigger"

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
    def test_execute_complete_success_task(self, check_job_status):
        """Assert execute_complete succeed"""
        check_job_status.return_value = JOB_ID
        assert (
            EMR_OPERATOR.execute_complete(
                context=None, event={"status": "success", "message": "Job completed", "job_id": JOB_ID}
            )
            == JOB_ID
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrContainerHook.submit_job")
    def test_execute_complete_fail_task(self, check_job_status):
        """Assert execute_complete throw AirflowException"""
        check_job_status.return_value = JOB_ID
        with pytest.raises(AirflowException):
            EMR_OPERATOR.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )
