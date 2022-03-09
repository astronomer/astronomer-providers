import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

from airflow.exceptions import AirflowException
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.databricks.hooks.databricks import DatabricksHookAsync


class DatabricksTrigger(BaseTrigger):  # noqa: D101
    def __init__(
        self,
        conn_id: str,
        task_id: str,
        run_id: str,
        retry_limit: int,
        retry_delay: int,
        polling_period_seconds: int,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.task_id = task_id
        self.run_id = run_id
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
                    yield TriggerEvent(True)
                    return
                else:
                    error_message = f"{self.task_id} failed with terminal state: {run_state}"
                    raise AirflowException(error_message)
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
