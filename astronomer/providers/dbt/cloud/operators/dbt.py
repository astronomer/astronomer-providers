import time
from typing import Any, Dict

from airflow import AirflowException
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

from astronomer.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger
from astronomer.providers.utils.typing_compat import Context


class DbtCloudRunJobOperatorAsync(DbtCloudRunJobOperator):
    """
    Executes a dbt Cloud job asynchronously. Trigger the dbt cloud job via worker to dbt and with run id in response
    poll for the status in trigger.

    .. seealso::
        For more information on sync Operator DbtCloudRunJobOperator, take a look at the guide:
        :ref:`howto/operator:DbtCloudRunJobOperator`

    :param dbt_cloud_conn_id: The connection ID for connecting to dbt Cloud.
    :param job_id: The ID of a dbt Cloud job.
    :param account_id: Optional. The ID of a dbt Cloud account.
    :param trigger_reason: Optional Description of the reason to trigger the job. Dbt requires the trigger reason while
        making an API. if it is not provided uses the default reasons.
    :param steps_override: Optional. List of dbt commands to execute when triggering the job instead of those
        configured in dbt Cloud.
    :param schema_override: Optional. Override the destination schema in the configured target for this job.
    :param timeout: Time in seconds to wait for a job run to reach a terminal status. Defaults to 7 days.
    :param check_interval: Time in seconds to check on a job run's status. Defaults to 60 seconds.
    :param additional_run_config: Optional. Any additional parameters that should be included in the API
        request when triggering the job.
    :return: The ID of the triggered dbt Cloud job run.
    """

    def execute(self, context: "Context") -> None:  # type: ignore[override]
        """Submits a job which generates a run_id and gets deferred"""
        if self.trigger_reason is None:
            self.trigger_reason = (
                f"Triggered via Apache Airflow by task {self.task_id!r} in the {self.dag.dag_id} DAG."
            )
        hook = DbtCloudHook(dbt_cloud_conn_id=self.dbt_cloud_conn_id)
        trigger_job_response = hook.trigger_job_run(
            account_id=self.account_id,
            job_id=self.job_id,
            cause=self.trigger_reason,
            steps_override=self.steps_override,
            schema_override=self.schema_override,
            additional_run_config=self.additional_run_config,
        )
        run_id = trigger_job_response.json()["data"]["id"]
        job_run_url = trigger_job_response.json()["data"]["href"]

        context["ti"].xcom_push(key="job_run_url", value=job_run_url)
        end_time = time.time() + self.timeout
        self.defer(
            timeout=self.execution_timeout,
            trigger=DbtCloudRunJobTrigger(
                conn_id=self.dbt_cloud_conn_id,
                run_id=run_id,
                end_time=end_time,
                account_id=self.account_id,
                poll_interval=self.check_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event: Dict[str, Any]) -> int:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])
        return int(event["run_id"])
