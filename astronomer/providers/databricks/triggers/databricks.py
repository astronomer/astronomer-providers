import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.databricks.hooks.databricks import DatabricksHookAsync


class DatabricksTrigger(BaseTrigger):
    """
    Wait asynchronously for databricks job to reach the terminal state.

    :param conn_id: The databricks connection id.
        The default value is ``databricks_default``.
    :param task_id: The task id.
    :param run_id: The databricks job run id.
    :param retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default, the operator will poll every 30 seconds.
    :param job_id: The databricks job id.
    :param run_page_url: The databricks run page url.
    """

    def __init__(
        self,
        conn_id: str,
        task_id: str,
        run_id: str,
        retry_limit: int,
        retry_delay: int,
        polling_period_seconds: int,
        job_id: Optional[int] = None,
        run_page_url: Optional[str] = None,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.task_id = task_id
        self.run_id = run_id
        self.job_id = job_id
        self.run_page_url = run_page_url
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay
        self.polling_period_seconds = polling_period_seconds

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes DatabricksTrigger arguments and classpath."""
        return (
            "astronomer.providers.databricks.triggers.databricks.DatabricksTrigger",
            {
                "conn_id": self.conn_id,
                "task_id": self.task_id,
                "run_id": self.run_id,
                "job_id": self.job_id,
                "run_page_url": self.run_page_url,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes a series of asynchronous http calls via a Databrick hook. It yields a Trigger if
        response is a 200 and run_state is successful, will retry the call up to the retry limit
        if the error is 'retryable', otherwise it throws an exception.
        """
        hook = self._get_async_hook()
        while True:
            run_state = await hook.get_run_state_async(self.run_id)
            if run_state.is_terminal:
                if run_state.is_successful:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "job_id": self.job_id,
                            "run_id": self.run_id,
                            "run_page_url": self.run_page_url,
                        }
                    )
                else:
                    error_message = f"{self.task_id} failed with terminal state: {run_state}"
                    yield TriggerEvent({"status": "error", "message": str(error_message)})
            else:
                self.log.info("%s in run state: %s", self.task_id, run_state)
                self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
                await asyncio.sleep(self.polling_period_seconds)

    def _get_async_hook(self) -> DatabricksHookAsync:
        return DatabricksHookAsync(
            self.conn_id,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
        )
