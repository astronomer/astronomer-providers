"""This module contains the Apache Livy Trigger."""
import asyncio
from typing import Any, AsyncIterator, Dict, Optional, Tuple, Union

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.apache.livy.hooks.livy import BatchState, LivyHookAsync


class LivyTrigger(BaseTrigger):
    """
    Check for the state of a previously submitted job with batch_id

    :param batch_id: Batch job id
    :param spark_params: Spark parameters; for example,
            spark_params = {"file": "test/pi.py", "class_name": "org.apache.spark.examples.SparkPi",
            "args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],"jars": "command-runner.jar",
            "driver_cores": 1, "executor_cores": 4,"num_executors": 1}
    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :param polling_interval: time in seconds between polling for job completion.  If poll_interval=0, in that case
        return the batch_id and if polling_interval > 0, poll the livy job for termination in the polling interval
        defined.
    :param extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    :param extra_headers: A dictionary of headers passed to the HTTP request to livy.
    :param livy_hook_async: LivyHookAsync object
    """

    def __init__(
        self,
        batch_id: Union[int, str],
        spark_params: Dict[Any, Any],
        livy_conn_id: str = "livy_default",
        polling_interval: int = 0,
        extra_options: Optional[Dict[str, Any]] = None,
        extra_headers: Optional[Dict[str, Any]] = None,
        livy_hook_async: Optional[LivyHookAsync] = None,
    ):
        super().__init__()
        self._batch_id = batch_id
        self.spark_params = spark_params
        self._livy_conn_id = livy_conn_id
        self._polling_interval = polling_interval
        self._extra_options = extra_options
        self._extra_headers = extra_headers
        self._livy_hook_async = livy_hook_async

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes LivyTrigger arguments and classpath."""
        return (
            "astronomer.providers.apache.livy.triggers.livy.LivyTrigger",
            {
                "batch_id": self._batch_id,
                "spark_params": self.spark_params,
                "livy_conn_id": self._livy_conn_id,
                "polling_interval": self._polling_interval,
                "extra_options": self._extra_options,
                "extra_headers": self._extra_headers,
                "livy_hook_async": self._livy_hook_async,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Checks if the _polling_interval > 0, in that case it pools Livy for
        batch termination asynchrnonously.
        else returns the success response
        """
        try:
            if self._polling_interval > 0:
                response = await self.poll_for_termination(self._batch_id)
                yield TriggerEvent(response)
            yield TriggerEvent(
                {
                    "status": "success",
                    "batch_id": self._batch_id,
                    "response": f"Batch {self._batch_id} succeeded",
                    "log_lines": None,
                }
            )
        except Exception as exc:
            yield TriggerEvent(
                {
                    "status": "error",
                    "batch_id": self._batch_id,
                    "response": f"Batch {self._batch_id} did not succeed with {str(exc)}",
                    "log_lines": None,
                }
            )

    async def poll_for_termination(self, batch_id: Union[int, str]) -> Dict[str, Any]:
        """
        Pool Livy for batch termination asynchrnonously.

        :param batch_id: id of the batch session to monitor.
        """
        hook = self._get_async_hook()
        state = await hook.get_batch_state(batch_id)
        self.log.info("Batch with id %s is in state: %s", batch_id, state["batch_state"].value)
        while state["batch_state"] not in hook.TERMINAL_STATES:
            self.log.info("Batch with id %s is in state: %s", batch_id, state["batch_state"].value)
            self.log.info("Sleeping for %s seconds", self._polling_interval)
            await asyncio.sleep(self._polling_interval)
            state = await hook.get_batch_state(batch_id)
        self.log.info("Batch with id %s terminated with state: %s", batch_id, state["batch_state"].value)
        log_lines = await hook.dump_batch_logs(batch_id)
        if state["batch_state"] != BatchState.SUCCESS:
            return {
                "status": "error",
                "batch_id": batch_id,
                "response": f"Batch {batch_id} did not succeed",
                "log_lines": log_lines,
            }
        return {
            "status": "success",
            "batch_id": batch_id,
            "response": f"Batch {batch_id} succeeded",
            "log_lines": log_lines,
        }

    def _get_async_hook(self) -> LivyHookAsync:
        if self._livy_hook_async is None or not isinstance(self._livy_hook_async, LivyHookAsync):
            self._livy_hook_async = LivyHookAsync(
                livy_conn_id=self._livy_conn_id,
                extra_headers=self._extra_headers,
                extra_options=self._extra_options,
            )
        return self._livy_hook_async
