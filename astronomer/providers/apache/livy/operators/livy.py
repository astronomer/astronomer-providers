"""This module contains the Apache Livy operator async."""
from typing import Any, Dict

from airflow.exceptions import AirflowException
from airflow.providers.apache.livy.operators.livy import LivyOperator

from astronomer.providers.apache.livy.triggers.livy import LivyTrigger
from astronomer.providers.utils.typing_compat import Context


class LivyOperatorAsync(LivyOperator):
    """
    This operator wraps the Apache Livy batch REST API, allowing to submit a Spark
    application to the underlying cluster asynchronously.

    :param file: path of the file containing the application to execute (required).
    :param class_name: name of the application Java/Spark main class.
    :param args: application command line arguments.
    :param jars: jars to be used in this sessions.
    :param py_files: python files to be used in this session.
    :param files: files to be used in this session.
    :param driver_memory: amount of memory to use for the driver process.
    :param driver_cores: number of cores to use for the driver process.
    :param executor_memory: amount of memory to use per executor process.
    :param executor_cores: number of cores to use for each executor.
    :param num_executors: number of executors to launch for this session.
    :param archives: archives to be used in this session.
    :param queue: name of the YARN queue to which the application is submitted.
    :param name: name of this session.
    :param conf: Spark configuration properties.
    :param proxy_user: user to impersonate when running the job.
    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :param polling_interval: time in seconds between polling for job completion. If poll_interval=0, in that case
        return the batch_id and if polling_interval > 0, poll the livy job for termination in the polling interval
        defined.
    :param extra_options: Additional option can be passed when creating a request.
            For example, ``run(json=obj)`` is passed as ``aiohttp.ClientSession().get(json=obj)``
    :param extra_headers: A dictionary of headers passed to the HTTP request to livy.
    :param retry_args: Arguments which define the retry behaviour.
            See Tenacity documentation at https://github.com/jd/tenacity
    """

    def execute(self, context: Context) -> None:
        """
        Airflow runs this method on the worker and defers using the trigger.
        Submit the job and get the job_id using which we defer and poll in trigger
        """
        self._batch_id = self.get_hook().post_batch(**self.spark_params)
        self.log.info("Generated batch-id is %s", self._batch_id)

        self.defer(
            timeout=self.execution_timeout,
            trigger=LivyTrigger(
                batch_id=self._batch_id,
                spark_params=self.spark_params,
                livy_conn_id=self._livy_conn_id,
                polling_interval=self._polling_interval,
                extra_options=self._extra_options,
                extra_headers=self._extra_headers,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: Dict[str, Any]) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        # dump the logs from livy to worker through triggerer.
        if event.get("log_lines", None) is not None:
            for log_line in event["log_lines"]:
                self.log.info(log_line)

        if event["status"] == "error":
            raise AirflowException(event["response"])
        self.log.info(
            "%s completed with response %s",
            self.task_id,
            event["response"],
        )
        return event["batch_id"]
