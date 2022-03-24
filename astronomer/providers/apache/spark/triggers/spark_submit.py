import asyncio
from typing import Any, AsyncIterator, Dict, List, Optional, Tuple

from airflow.triggers.base import BaseTrigger, TriggerEvent

from astronomer.providers.apache.spark.hooks.spark_submit import SparkSubmitHookAsync


class SparkSubmitTrigger(BaseTrigger):
    """
    SparkSubmitTrigger submit the spark job in async and poll for the status

    :param task_id: task id which the trigger need to be triggered
    :param polling_period_seconds: polling period
    :param application: The application that submitted as a job, either jar or py file. (templated)
    :param conf: Arbitrary Spark configuration properties (templated)
    :param conn_id: refer `spark connection id <howto/connection:spark>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default to yarn.
    :param files: Upload additional files to the executor running the job, separated by a
                  comma. Files will be placed in the working directory of each executor.
                  For example, serialized objects. (templated)
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py. (templated)
    :param archives: Archives that spark should unzip (and possibly tag with #ALIAS) into
        the application working directory.
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

    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        application: str,
        conn_id: str,
        name: str,
        conf: Optional[Dict[str, Any]] = None,
        files: Optional[str] = None,
        py_files: Optional[str] = None,
        archives: Optional[str] = None,
        driver_class_path: Optional[str] = None,
        jars: Optional[str] = None,
        java_class: Optional[str] = None,
        packages: Optional[str] = None,
        exclude_packages: Optional[str] = None,
        repositories: Optional[str] = None,
        total_executor_cores: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        driver_memory: Optional[str] = None,
        keytab: Optional[str] = None,
        principal: Optional[str] = None,
        proxy_user: Optional[str] = None,
        num_executors: Optional[int] = None,
        application_args: Optional[List[Any]] = None,
        env_vars: Optional[Dict[str, Any]] = None,
        verbose: bool = False,
        spark_binary: Optional[str] = None,
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.application = application
        self.conf = conf
        self.conn_id = conn_id
        self.files = files
        self.py_files = py_files
        self.archives = archives
        self.driver_class_path = driver_class_path
        self.jars = jars
        self.java_class = java_class
        self.packages = packages
        self.exclude_packages = exclude_packages
        self.repositories = repositories
        self.total_executor_cores = total_executor_cores
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.keytab = keytab
        self.principal = principal
        self.proxy_user = proxy_user
        self.name = name
        self.num_executors = num_executors
        self.application_args = application_args
        self.env_vars = env_vars
        self.verbose = verbose
        self.spark_binary = spark_binary

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes SparkSubmitTrigger arguments and classpath."""
        return (
            "astronomer.providers.apache.spark.triggers.spark_submit.SparkSubmitTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "application": self.application,
                "conn_id": self.conn_id,
                "name": self.name,
                "conf": self.conf,
                "files": self.files,
                "py_files": self.py_files,
                "archives": self.archives,
                "driver_class_path": self.driver_class_path,
                "jars": self.jars,
                "java_class": self.java_class,
                "packages": self.packages,
                "exclude_packages": self.exclude_packages,
                "repositories": self.repositories,
                "total_executor_cores": self.total_executor_cores,
                "executor_cores": self.executor_cores,
                "executor_memory": self.executor_memory,
                "driver_memory": self.driver_memory,
                "keytab": self.keytab,
                "principal": self.principal,
                "proxy_user": self.proxy_user,
                "num_executors": self.num_executors,
                "application_args": self.application_args,
                "env_vars": self.env_vars,
                "verbose": self.verbose,
                "spark_binary": self.spark_binary,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Makes a series of connections to snowflake to get the status of the query
        by async get_query_status function
        """
        hook = self._get_async_hook()
        try:
            await hook.spark_submit_job(self.application)
            while hook.still_running():
                await asyncio.sleep(self.polling_period_seconds)
            driver_status = hook.get_driver_status()
            if driver_status:
                yield TriggerEvent(driver_status)
                return
            else:
                error_message = f"{self.task_id} failed with terminal"
                yield TriggerEvent({"status": "ERROR", "message": str(error_message)})
                return
        except Exception as e:
            yield TriggerEvent({"status": "ERROR", "message": str(e)})
            return

    def _get_async_hook(self) -> SparkSubmitHookAsync:
        """Return the SparkSubmitHookAsync class"""
        return SparkSubmitHookAsync(
            conf=self.conf,
            conn_id=self.conn_id,
            files=self.files,
            py_files=self.py_files,
            archives=self.archives,
            driver_class_path=self.driver_class_path,
            jars=self.jars,
            java_class=self.java_class,
            packages=self.packages,
            exclude_packages=self.exclude_packages,
            repositories=self.repositories,
            total_executor_cores=self.total_executor_cores,
            executor_cores=self.executor_cores,
            executor_memory=self.executor_memory,
            driver_memory=self.driver_memory,
            keytab=self.keytab,
            principal=self.principal,
            proxy_user=self.proxy_user,
            name=self.name,
            num_executors=self.num_executors,
            application_args=self.application_args,
            env_vars=self.env_vars,
            verbose=self.verbose,
            spark_binary=self.spark_binary,
        )
