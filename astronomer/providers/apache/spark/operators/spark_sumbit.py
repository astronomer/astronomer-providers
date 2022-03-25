from typing import Any, Dict, List, Optional, Union

from airflow import AirflowException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.context import Context

from astronomer.providers.apache.spark.triggers.spark_submit import SparkSubmitTrigger


class SparkSubmitOperatorAsync(SparkSubmitOperator):
    """
    This is an Async Operator for the spark-submit binary to kick off a spark-submit job.
    It requires that the "spark-submit" binary is in the PATH or the spark-home is set
    in the extra on the connection.

    :param application: The application that submitted as a job, either jar or py file. (templated)
    :param conf: Arbitrary Spark configuration properties (templated)
    :param conn_id: refer `spark connection id <howto/connection:spark>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default to yarn.
    :param files: Upload additional files to the executor running the job, separated by a
                  comma. Files will be placed in the working directory of each executor.
                  For example, serialized objects. (templated)
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py. (templated)
    :param jars: Submit additional jars to upload and place them in executor classpath. (templated)
    :param driver_class_path: Additional, driver-specific, classpath settings. (templated)
    :param java_class: the main class of the Java application
    :param packages: Comma-separated list of maven coordinates of jars to include on the
                     driver and executor classpaths. (templated)
    :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
                             while resolving the dependencies provided in 'packages' (templated)
    :param repositories: Comma-separated list of additional remote repositories to search
                         for the maven coordinates given with 'packages'
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
                                 (Default: all the available cores on the worker)
    :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab (templated)
    :param principal: The name of the kerberos principal used for keytab (templated)
    :param proxy_user: User to impersonate when submitting the application (templated)
    :param name: Name of the job (default airflow-spark). (templated)
    :param num_executors: Number of executors to launch
    :param application_args: Arguments for the application being submitted (templated)
    :param env_vars: Environment variables for spark-submit. It supports yarn and k8s mode too. (templated)
    :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
    :param spark_binary: The command to use for spark submit.
                         Some distros may use spark2-submit.
    """

    def __init__(self, *, poll_interval: int = 5, **kwargs: Any) -> None:
        self.poll_interval = poll_interval
        super().__init__(**kwargs)

    def execute(self, context: "Context") -> None:
        """
        Airflow runs this method on the worker and defers using the trigger and make a call to
        SparkSubmitHookAsync to run the provided spark job
        """
        self.defer(
            timeout=self.execution_timeout,
            trigger=SparkSubmitTrigger(
                task_id=self.task_id,
                polling_period_seconds=self.poll_interval,
                application=self._application,
                conf=self._conf,
                conn_id=self._conn_id,
                files=self._files,
                py_files=self._py_files,
                archives=self._archives,
                driver_class_path=self._driver_class_path,
                jars=self._jars,
                java_class=self._java_class,
                packages=self._packages,
                exclude_packages=self._exclude_packages,
                repositories=self._repositories,
                total_executor_cores=self._total_executor_cores,
                executor_cores=self._executor_cores,
                executor_memory=self._executor_memory,
                driver_memory=self._driver_memory,
                keytab=self._keytab,
                principal=self._principal,
                proxy_user=self._proxy_user,
                name=self._name,
                num_executors=self._num_executors,
                application_args=self._application_args,
                env_vars=self._env_vars,
                verbose=self._verbose,
                spark_binary=self._spark_binary,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Dict[str, Any], event: Optional[Dict[str, Union[str, List[str]]]] = None
    ) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if event["status"] == "ERROR":
                raise AirflowException(event["message"])
        self.log.info("%s completed successfully.", self.task_id)
        return None
