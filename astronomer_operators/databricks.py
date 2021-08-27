import asyncio
from typing import Any, Dict, Tuple

from airflow.exceptions import AirflowException
from airflow.providers.databricks.operators.databricks import (
    XCOM_RUN_ID_KEY,
    XCOM_RUN_PAGE_URL_KEY,
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer_operators.hooks.databricks import DatabricksHookAsync


class DatabricksSubmitRunOperatorAsync(DatabricksSubmitRunOperator):
    def execute(self, context):
        """
        Logic that the operator uses to execute the Databricks trigger,
        and defer execution as expected. It makes two non-async API calls to
        submit the run, and retrieve the run page URL. It also pushes these
        values as xcom data if do_xcom_push is set to True in the context.
        """
        # Note: This hook makes non-async calls.
        # It is imported from the Databricks base class.
        # Async calls (i.e. polling) are handled in the Trigger.
        hook = self._get_hook()
        self.run_id = hook.submit_run(self.json)

        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=self.run_id)
        self.log.info("Run submitted with run_id: %s", self.run_id)
        self.run_page_url = hook.get_run_page_url(self.run_id)
        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=self.run_page_url)

        self.log.info("View run status, Spark UI, and logs at %s", self.run_page_url)

        self.defer(
            timeout=self.execution_timeout,
            trigger=DatabricksTrigger(
                conn_id=self.databricks_conn_id,
                task_id=self.task_id,
                run_id=self.run_id,
                retry_limit=self.databricks_retry_limit,
                retry_delay=self.databricks_retry_delay,
                polling_period_seconds=self.polling_period_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None


class DatabricksRunNowOperatorAsync(DatabricksRunNowOperator):
    def execute(self, context):
        """
        Logic that the operator uses to execute the Databricks trigger,
        and defer execution as expected. It makes two non-async API calls to
        submit the run, and retrieve the run page URL. It also pushes these
        values as xcom data if do_xcom_push is set to True in the context.
        """
        # Note: This hook makes non-async calls.
        # It is from the Databricks base class.
        hook = self._get_hook()
        self.run_id = hook.run_now(self.json)

        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=self.run_id)
        self.log.info("Run submitted with run_id: %s", self.run_id)
        self.run_page_url = hook.get_run_page_url(self.run_id)
        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=self.run_page_url)

        self.log.info("View run status, Spark UI, and logs at %s", self.run_page_url)

        self.defer(
            timeout=self.execution_timeout,
            trigger=DatabricksTrigger(
                task_id=self.task_id,
                conn_id=self.databricks_conn_id,
                run_id=self.run_id,
                retry_limit=self.databricks_retry_limit,
                retry_delay=self.databricks_retry_delay,
                polling_period_seconds=self.polling_period_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event=None):  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        self.log.info("%s completed successfully.", self.task_id)
        return None


class DatabricksTrigger(BaseTrigger):
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
        """
        Serializes DatabricksTrigger arguments and classpath.
        """
        return (
            "astronomer_operators.databricks.DatabricksTrigger",
            {
                "conn_id": self.conn_id,
                "task_id": self.task_id,
                "run_id": self.run_id,
                "retry_limit": self.retry_limit,
                "retry_delay": self.retry_delay,
                "polling_period_seconds": self.polling_period_seconds,
            },
        )

    async def run(self):
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
                    error_message = (
                        f"{self.task_id} failed with terminal state: {run_state}"
                    )
                    raise AirflowException(error_message)
            else:
                # TODO: Figure out logging for trigger
                self.log.info("%s in run state: %s", self.task_id, run_state)
                self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
                await asyncio.sleep(self.polling_period_seconds)

    def _get_async_hook(self) -> DatabricksHookAsync:
        return DatabricksHookAsync(
            self.conn_id,
            retry_limit=self.retry_limit,
            retry_delay=self.retry_delay,
        )
