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
    :param end_time: Time in seconds to wait for a job run to reach a terminal status. Defaults to 7 days.
    :param account_id: The ID of a dbt Cloud account.
    :param poll_interval:  polling period in seconds to check for the status.
    """

    def __init__(
        self,
        conn_id: str,
        run_id: int,
        end_time: float,
        poll_interval: float,
        account_id: Optional[int],
    ):
        super().__init__()
        self.run_id = run_id
        self.account_id = account_id
        self.conn_id = conn_id
        self.end_time = end_time
        self.poll_interval = poll_interval

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
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Make async connection to Dbt, polls for the pipeline run status"""
        hook = DbtCloudHookAsync(self.conn_id)
        try:
            while await self.is_still_running(hook):
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
            if job_run_status == DbtCloudJobRunStatus.SUCCESS.value:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "message": f"Job run {self.run_id} has completed successfully.",
                        "run_id": self.run_id,
                    }
                )
            elif job_run_status == DbtCloudJobRunStatus.CANCELLED.value:
                yield TriggerEvent(
                    {
                        "status": "cancelled",
                        "message": f"Job run {self.run_id} has been cancelled.",
                        "run_id": self.run_id,
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Job run {self.run_id} has failed.",
                        "run_id": self.run_id,
                    }
                )
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e), "run_id": self.run_id})

    async def is_still_running(self, hook: DbtCloudHookAsync) -> bool:
        """
        Async function to check whether the job is submitted via async API is in
        running state and returns True if it is still running else
        return False
        """
        job_run_status = await hook.get_job_status(self.run_id, self.account_id)
        if not DbtCloudJobRunStatus.is_terminal(job_run_status):
            return True
        return False
