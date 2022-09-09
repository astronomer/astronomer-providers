import time
from typing import Any, Dict, Optional

from airflow import AirflowException
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.context import Context

from astronomer.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger


class DbtCloudRunJobOperatorAsync(DbtCloudRunJobOperator):
    """
    Executes a dbt Cloud job asynchronously. Trigger the dbt cloud job via worker to dbt and with run id in response
    poll for the status in trigger.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DbtCloudRunJobOperator`

    :param dbt_cloud_conn_id: The connection ID for connecting to dbt Cloud.
    :param job_id: The ID of a dbt Cloud job.
    :param account_id: Optional. The ID of a dbt Cloud account.
    :param trigger_reason: Description of the reason to trigger the job.
    :param steps_override: Optional. List of dbt commands to execute when triggering the job instead of those
        configured in dbt Cloud.
    :param schema_override: Optional. Override the destination schema in the configured target for this job.
    :param wait_for_termination: This Flag is disabled in this async operator.wait_for_termination
        will be always set True
    :param timeout: Time in seconds to wait for a job run to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True. Defaults to 7 days.
    :param check_interval: Time in seconds to check on a job run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True. Defaults to 60 seconds.
    :param additional_run_config: Optional. Any additional parameters that should be included in the API
        request when triggering the job.
    :return: The ID of the triggered dbt Cloud job run.
    """

    def execute(self, context: "Context") -> None:  # type: ignore[override]
        """Submits a job which generates a run_id and gets deferred"""
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
                run_id=run_id,
                wait_for_termination=True,
                conn_id=self.dbt_cloud_conn_id,
                account_id=self.account_id,
                poll_interval=self.check_interval,
                end_time=end_time,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Dict[Any, Any], event: Optional[Dict[str, Any]]) -> Optional[int]:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event and "status" in event:
            if event["status"] == "error":
                raise AirflowException(event["message"])
            self.log.info(event["message"])
            run_id: int = event["run_id"]
            return run_id
        return None
