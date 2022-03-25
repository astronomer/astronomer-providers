import asyncio
from typing import Any, Dict, Optional

from airflow import AirflowException
from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook


class SparkSubmitHookAsync(SparkSubmitHook):
    """
    This Async hook is a wrapper around the spark-submit binary to kick off a spark-submit job.
    It requires that the "spark-submit" binary is in the PATH or the spark_home to be
    supplied.

    :param conf: Arbitrary Spark configuration properties
    :param conn_id: refer `spark connection id <howto/connection:spark>` as configured
        in Airflow administration. When an invalid connection_id is supplied, it will default
        to yarn.
    :param files: Upload additional files to the executor running the job, separated by a
        comma. Files will be placed in the working directory of each executor.
        For example, serialized objects.
    :param py_files: Additional python files used by the job, can be .zip, .egg or .py.
    :param: archives: Archives that spark should unzip (and possibly tag with #ALIAS) into
        the application working directory.
    :param driver_class_path: Additional, driver-specific, classpath settings.
    :param jars: Submit additional jars to upload and place them in executor classpath.
    :param java_class: the main class of the Java application
    :param packages: Comma-separated list of maven coordinates of jars to include on the
        driver and executor classpaths
    :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
        while resolving the dependencies provided in 'packages'
    :param repositories: Comma-separated list of additional remote repositories to search
        for the maven coordinates given with 'packages'
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
        (Default: all the available cores on the worker)
    :param executor_cores: (Standalone, YARN and Kubernetes only) Number of cores per
        executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab
    :param principal: The name of the kerberos principal used for keytab
    :param proxy_user: User to impersonate when submitting the application
    :param name: Name of the job (default airflow-spark)
    :param num_executors: Number of executors to launch
    :param status_poll_interval: Seconds to wait between polls of driver status in cluster
        mode (Default: 1)
    :param application_args: Arguments for the application being submitted
    :param env_vars: Environment variables for spark-submit. It
        supports yarn and k8s mode too.
    :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
    :param spark_binary: The command to use for spark submit.
                         Some distros may use spark2-submit.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    async def spark_submit_job(self, application: str, **kwargs: Any) -> None:
        """
        Asyncio subprocess create_subprocess_shell to execute the spark-submit job

        :param application: Submitted application, jar or py file
        :param kwargs: extra arguments to create_subprocess_shell (see subprocess.create_subprocess_shell)
        """
        spark_submit_cmd = self._build_spark_submit_command(
            application
        )  # inherit from the SparkSubmitHook class
        # to build the spark submit command

        spark_cmd = " ".join(spark_submit_cmd)

        self._submit_sp = await asyncio.create_subprocess_shell(
            spark_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            bufsize=0,
            **kwargs,
        )
        self.log.debug("Started subprocess: %r", self._submit_sp)
        await asyncio.wait(
            [
                asyncio.create_task(self._process_submit_log(self._submit_sp.stdout)),
                asyncio.create_task(self._process_submit_log(self._submit_sp.stderr)),
            ]
        )
        return_code = self._submit_sp.returncode

        if return_code:
            raise AirflowException(
                f"Cannot execute: {self._mask_cmd(spark_submit_cmd)}. Error code is: {return_code}."
            )
        if (
            return_code is None
            and self._connection["master"] == "local[*]"
            and self._should_track_driver_status is False
        ):
            self._driver_status = "FINISHED"

    async def _process_submit_log(self, stream: Any) -> None:
        """
        Processes the log and extracts useful information out of it.

        :param stream: subprocess stream bytes
        """
        while True:
            line = await stream.readline()
            if line:
                line = line.decode().strip()
                line = line.strip()
                self.log.info(line)
                # TODO
                # Need to implement the cluster mode yarn, standalone cluster mode, kubernetes
                # Need to get the application id if cluster_mode is in yarn mode
                # Need to get the driver id if cluster_mode is in standalone and kubernetes
            else:
                break

    def track_driver_status(self) -> Optional[str]:
        """Track driver status by driver id"""
        if self._should_track_driver_status:
            if self._driver_id is None:
                raise AirflowException(
                    "No driver id is known: something went wrong when executing the spark submit command"
                )
            # TODO
            # Need to implement the cluster mode yarn, standalone cluster mode, kubernetes
        return self._driver_status

    def still_running(self) -> bool:
        """
        Check for the driver status if status not in ["FINISHED", "UNKNOWN", "KILLED", "FAILED", "ERROR"]
         will return true, if not false which is considered the job is still running
        """
        driver_status = self.track_driver_status()
        if driver_status not in ["FINISHED", "UNKNOWN", "KILLED", "FAILED", "ERROR"]:
            return True
        return False

    def get_driver_status(self) -> Dict[str, Optional[str]]:
        """Get the driver status and based on the driver status returns the message and status"""
        msg = ""
        if self._driver_status == "SUBMITTED":
            msg = "Submitted but not yet scheduled on a worker"
        elif self._driver_status == "RUNNING":
            msg = "Has been allocated to a worker to run"
        elif self._driver_status == "FINISHED":
            msg = "Previously ran and exited cleanly"
        elif self._driver_status == "RELAUNCHING":
            msg = "Exited non-zero or due to worker failure, but has not yet started running again"
        elif self._driver_status == "UNKNOWN":
            msg = "The status of the driver is temporarily not known due to master failure recovery"
        elif self._driver_status == "KILLED":
            msg = "A user manually killed this driver"
        elif self._driver_status == "FAILED":
            msg = "The driver exited non-zero and was not supervised"
        elif self._driver_status == "ERROR":
            msg = "Unable to run or restart due to an unrecoverable error"
        return {"status": self._driver_status, "message": msg}
