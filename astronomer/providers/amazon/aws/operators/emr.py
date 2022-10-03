from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook
from airflow.providers.amazon.aws.operators.emr import EmrContainerOperator

from astronomer.providers.amazon.aws.triggers.emr import EmrContainerOperatorTrigger
from astronomer.providers.utils.typing_compat import Context


class EmrContainerOperatorAsync(EmrContainerOperator):
    """
    An async operator that submits jobs to EMR on EKS virtual clusters.

    :param name: The name of the job run.
    :param virtual_cluster_id: The EMR on EKS virtual cluster ID
    :param execution_role_arn: The IAM role ARN associated with the job run.
    :param release_label: The Amazon EMR release version to use for the job run.
    :param job_driver: Job configuration details, e.g. the Spark job parameters.
    :param configuration_overrides: The configuration overrides for the job run,
        specifically either application configuration or monitoring configuration.
    :param client_request_token: The client idempotency token of the job run request.
        Use this if you want to specify a unique ID to prevent two jobs from getting started.
        If no token is provided, a UUIDv4 token will be generated for you.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check query status on EMR
    :param max_tries: Deprecated - use max_polling_attempts instead.
    :param max_polling_attempts: Maximum number of times to wait for the job run to finish.
        Defaults to None, which will poll until the job is *not* in a pending, submitted, or running state.
    :param tags: The tags assigned to job runs. Defaults to None
    """

    def execute(self, context: Context) -> None:
        """Deferred and give control to trigger"""
        hook = EmrContainerHook(aws_conn_id=self.aws_conn_id, virtual_cluster_id=self.virtual_cluster_id)
        job_id = hook.submit_job(
            name=self.name,
            execution_role_arn=self.execution_role_arn,
            release_label=self.release_label,
            job_driver=self.job_driver,
            configuration_overrides=self.configuration_overrides,
            client_request_token=self.client_request_token,
        )
        try:
            # for apache-airflow-providers-amazon<6.0.0
            polling_attempts = self.max_tries  # type: ignore[attr-defined]
        except AttributeError:  # pragma: no cover
            # for apache-airflow-providers-amazon>=6.0.0
            # max_tries is deprecated so instead of max_tries using self.max_polling_attempts
            polling_attempts = self.max_polling_attempts

        self.defer(
            timeout=self.execution_timeout,
            trigger=EmrContainerOperatorTrigger(
                virtual_cluster_id=self.virtual_cluster_id,
                job_id=job_id,
                aws_conn_id=self.aws_conn_id,
                poll_interval=self.poll_interval,
                max_tries=polling_attempts,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> str:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if "status" in event and event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])
        job_id: str = event["job_id"]
        return job_id
