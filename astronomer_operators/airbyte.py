import asyncio
from typing import Any, Dict, Tuple

from airflow.exceptions import AirflowException
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.hooks.airbyte import AirbyteHookAsync


class AirbyteTriggerAsyncOperator(AirbyteTriggerSyncOperator):

    def execute(self, context) -> None:
        """Create Airbyte Job and wait to finish"""
        hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)
        job_object = hook.submit_sync_connection(connection_id=self.connection_id)
        job_id = job_object.json()['job']['id']

        self.log.info("Job %s was submitted to Airbyte Server", job_id)

        self.defer(
            timeout=self.execution_timeout,
            trigger=AirbyteTrigger(
                conn_id=self.airbyte_conn_id,
                task_id=self.task_id,
                job_id=job_id,
                wait_seconds=self.wait_seconds,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("Job completed successfully.")
        self.log.info(context)
        self.log.info(event)
        return None


class AirbyteTrigger(BaseTrigger):
    def __init__(
            self,
            conn_id: str,
            task_id: str,
            job_id: str,
            wait_seconds: int,
            timeout: int,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.task_id = task_id
        self.job_id = job_id
        self.wait_seconds = wait_seconds
        self.timeout = timeout

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """
        Serializes AirbyteTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.airbyte.AirbyteTrigger",
            {
                "conn_id": self.conn_id,
                "task_id": self.task_id,
                "job_id": self.job_id,
                "wait_seconds": self.wait_seconds,
                "timeout": self.timeout,
            },
        )

    async def run(self):
        """
        Makes a series of asynchronous http calls via a Airbyte hook. It yields a Trigger if
        response is a 200 and run_state is successful, will retry the call up to the timeout
        if the error is 'retryable', otherwise it throws an exception.
        """
        hook = self._get_async_hook()
        while True:
            run_state = await hook.get_run_state_async(self.job_id)
            state = run_state["job"]["status"]

            if state in (hook.RUNNING, hook.PENDING, hook.INCOMPLETE):
                self.log.info("%s in run state: %s", self.job_id, state)
                self.log.info("Sleeping for %s seconds.", self.wait_seconds)
                await asyncio.sleep(self.wait_seconds)
            elif state == hook.SUCCEEDED:
                yield TriggerEvent(True)
                return
            elif state == hook.ERROR:
                raise AirflowException(f"Job failed:\n{self.job_id}")
            elif state == hook.CANCELLED:
                raise AirflowException(f"Job was cancelled:\n{self.job_id}")
            else:
                raise Exception(f"Encountered unexpected state `{state}` for job_id `{self.job_id}`")

    def _get_async_hook(self) -> AirbyteHookAsync:
        return AirbyteHookAsync(
            self.conn_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )
