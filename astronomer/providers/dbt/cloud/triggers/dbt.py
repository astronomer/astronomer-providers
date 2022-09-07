import asyncio
import time
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudJobRunStatus
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.dbt.cloud.hooks.dbt import DbtCloudHookAsync


class DbtCloudRunJobTrigger(BaseTrigger):
    """
    DbtCloudRunJobTrigger is triggered with run id and account id, makes async Http call to dbt and get the status
    for the submitted job with run id in polling interval of time.

    :param conn_id: The connection identifier for connecting to Dbt.
    :param run_id: The ID of a dbt Cloud job.
    :param account_id: The ID of a dbt Cloud account.
    :param poll_interval:  polling period in seconds to check for the status.
    :param timeout: Time in seconds to wait for a job run to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True. Defaults to 7 days.
    """

    def __init__(
        self,
        conn_id: str,
        poll_interval: float,
        end_time: float,
        run_id: Optional[int],
        account_id: Optional[int],
        wait_for_termination: bool = True,
    ):
        super().__init__()
        self.run_id = run_id
        self.account_id = account_id
        self.conn_id = conn_id
        self.end_time = end_time
        self.poll_interval = poll_interval
        self.wait_for_termination = wait_for_termination

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes DbtCloudRunJobTrigger arguments and classpath."""
        return (
            "astronomer.providers.dbt.cloud.triggers.dbt.DbtCloudRunJobTrigger",
            {
                "run_id": self.run_id,
                "account_id": self.account_id,
                "conn_id": self.conn_id,
                "end_time": self.end_time,
                "poll_interval": self.poll_interval,
                "wait_for_termination": self.wait_for_termination,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make async connection to Dbt, polls for the pipeline run status"""
        hook = DbtCloudHookAsync(self.conn_id)
        try:
            job_run_status = await hook.get_job_status(self.run_id, self.account_id)

            while (
                not DbtCloudJobRunStatus.is_terminal(job_run_status)
                and job_run_status not in DbtCloudJobRunStatus.SUCCESS.value
            ):
                if self.end_time < time.time():
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": f"Job run {self.run_id} has not reached a terminal status after "
                            f"{self.end_time} seconds.",
                            "run_id": self.run_id,
                        }
                    )
                await asyncio.sleep(self.poll_interval)

                job_run_status = await hook.get_job_status(self.run_id, self.account_id)

            if job_run_status in DbtCloudJobRunStatus.SUCCESS.value:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"Job run {self.run_id} has completed successfully.",
                        "run_id": self.run_id,
                    }
                )
            return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})
