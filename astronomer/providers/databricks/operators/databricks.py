from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.databricks.operators.databricks import (
    XCOM_RUN_ID_KEY,
    XCOM_RUN_PAGE_URL_KEY,
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)

from astronomer.providers.databricks.triggers.databricks import DatabricksTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksSubmitRunOperatorAsync(DatabricksSubmitRunOperator):
    """
    Submits a Spark job run asynchronously to Databricks using the
    `api/2.1/jobs/runs/submit
    <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.1/jobs/runs/submit`` endpoint and pass it directly
    to our ``DatabricksSubmitRunOperator`` through the ``json`` parameter.
    For example ::

        json = {
          'new_cluster': {
            'spark_version': '2.1.0-db3-scala2.11',
            'num_workers': 2
          },
          'notebook_task': {
            'notebook_path': '/Users/airflow@example.com/PrepareData',
          },
        }
        notebook_run = DatabricksSubmitRunOperator(task_id='notebook_run', json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``runs/submit``
    endpoint. In this method, your code would look like this: ::

        new_cluster = {
          'spark_version': '10.1.x-scala2.12',
          'num_workers': 2
        }
        notebook_task = {
          'notebook_path': '/Users/airflow@example.com/PrepareData',
        }
        notebook_run = DatabricksSubmitRunOperator(
            task_id='notebook_run',
            new_cluster=new_cluster,
            notebook_task=notebook_task)

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksSubmitRunOperator`` supports are
        - ``spark_jar_task``
        - ``notebook_task``
        - ``spark_python_task``
        - ``spark_jar_task``
        - ``spark_submit_task``
        - ``pipeline_task``
        - ``new_cluster``
        - ``existing_cluster_id``
        - ``libraries``
        - ``run_name``
        - ``timeout_seconds``

    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/runs/submit`` endpoint. The other named parameters
        (i.e. ``spark_jar_task``, ``notebook_task``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        ..seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit
    :param spark_jar_task: The main class and parameters for the JAR task. Note that
        the actual JAR is specified in the ``libraries``.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkjartask
    :param notebook_task: The notebook path and parameters for the notebook task.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsnotebooktask
    :param spark_python_task: The python file path and parameters to run the python file with.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkpythontask
    :param spark_submit_task: Parameters needed to run a spark-submit command.
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparksubmittask
    :param pipeline_task: Parameters needed to execute a Delta Live Tables pipeline task.
        The provided dictionary must contain at least ``pipeline_id`` field!
        *EITHER* ``spark_jar_task`` *OR* ``notebook_task`` *OR* ``spark_python_task``
        *OR* ``spark_submit_task`` *OR* ``pipeline_task`` should be specified.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobspipelinetask
    :param new_cluster: Specs for a new cluster on which this task will be run.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified
        (except when ``pipeline_task`` is used).
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsclusterspecnewcluster
    :param existing_cluster_id: ID for existing cluster on which to run this task.
        *EITHER* ``new_cluster`` *OR* ``existing_cluster_id`` should be specified
        (except when ``pipeline_task`` is used).
        This field will be templated.
    :param libraries: Libraries which this run will use.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/2.0/jobs.html#managedlibrarieslibrary
    :param run_name: The run name used for this task.
        By default, this will be set to the Airflow ``task_id``. This ``task_id`` is a
        required parameter of the superclass ``BaseOperator``.
        This field will be templated.
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param databricks_conn_id: The databricks connection id.
        The default value is ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default, the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    """

    def execute(self, context: "Context") -> None:
        """
        Execute the Databricks trigger, and defer execution as expected. It makes two non-async API calls to
        submit the run, and retrieve the run page URL. It also pushes these
        values as xcom data if do_xcom_push is set to True in the context.
        """
        # Note: This hook makes non-async calls.
        # It is imported from the Databricks base class.
        # Async calls (i.e. polling) are handled in the Trigger.
        hook = self._get_hook()
        self.run_id = hook.submit_run(self.json)
        job_id = hook.get_job_id(self.run_id)

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
                run_id=str(self.run_id),
                job_id=job_id,
                run_page_url=self.run_page_url,
                retry_limit=self.databricks_retry_limit,
                retry_delay=self.databricks_retry_delay,
                polling_period_seconds=self.polling_period_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event: Any = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("%s completed successfully.", self.task_id)
        if event.get("job_id"):
            context["ti"].xcom_push(key="job_id", value=event["job_id"])

        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=event.get("run_id"))
            context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=event.get("run_page_url"))


class DatabricksRunNowOperatorAsync(DatabricksRunNowOperator):
    """
    Runs an existing Spark job run asynchronously to Databricks using the
    `api/2.1/jobs/run-now
    <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow>`_
    API endpoint.

    There are two ways to instantiate this operator.

    In the first way, you can take the JSON payload that you typically use
    to call the ``api/2.1/jobs/run-now`` endpoint and pass it directly
    to our ``DatabricksRunNowOperator`` through the ``json`` parameter.
    For example ::

        json = {
          "job_id": 42,
          "notebook_params": {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
          }
        }

        notebook_run = DatabricksRunNowOperator(task_id='notebook_run', json=json)

    Another way to accomplish the same thing is to use the named parameters
    of the ``DatabricksRunNowOperator`` directly. Note that there is exactly
    one named parameter for each top level parameter in the ``run-now``
    endpoint. In this method, your code would look like this: ::

        job_id=42

        notebook_params = {
            "dry-run": "true",
            "oldest-time-to-consider": "1457570074236"
        }

        python_params = ["douglas adams", "42"]

        jar_params = ["douglas adams", "42"]

        spark_submit_params = ["--class", "org.apache.spark.examples.SparkPi"]

        notebook_run = DatabricksRunNowOperator(
            job_id=job_id,
            notebook_params=notebook_params,
            python_params=python_params,
            jar_params=jar_params,
            spark_submit_params=spark_submit_params
        )

    In the case where both the json parameter **AND** the named parameters
    are provided, they will be merged together. If there are conflicts during the merge,
    the named parameters will take precedence and override the top level ``json`` keys.

    Currently the named parameters that ``DatabricksRunNowOperator`` supports are
        - ``job_id``
        - ``job_name``
        - ``json``
        - ``notebook_params``
        - ``python_params``
        - ``jar_params``
        - ``spark_submit_params``


    :param job_id: the job_id of the existing Databricks job.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param job_name: the name of the existing Databricks job.
        It must exist only one job with the specified name.
        ``job_id`` and ``job_name`` are mutually exclusive.
        This field will be templated.
    :param json: A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/run-now`` endpoint. The other named parameters
        (i.e. ``notebook_params``, ``spark_submit_params``..) to this operator will
        be merged with this json dictionary if they are provided.
        If there are conflicts during the merge, the named parameters will
        take precedence and override the top level json keys. (templated)

        ..seealso::
            For more information about templating see :ref:`concepts:jinja-templating`.
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param notebook_params: A dict from keys to values for jobs with notebook task,
        e.g. "notebook_params": {"name": "john doe", "age":  "35"}.
        The map is passed to the notebook and will be accessible through the
        dbutils.widgets.get function. See Widgets for more information.
        If not specified upon run-now, the triggered run will use the
        job’s base parameters. notebook_params cannot be
        specified in conjunction with jar_params. The json representation
        of this field (i.e. {"notebook_params":{"name":"john doe","age":"35"}})
        cannot exceed 10,000 bytes.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/user-guide/notebooks/widgets.html
    :param python_params: A list of parameters for jobs with python tasks,
        e.g. "python_params": ["john doe", "35"].
        The parameters will be passed to python file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in
        job setting.
        The json representation of this field (i.e. {"python_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param jar_params: A list of parameters for jobs with JAR tasks,
        e.g. "jar_params": ["john doe", "35"].
        The parameters will be passed to JAR file as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified in
        job setting.
        The json representation of this field (i.e. {"jar_params":["john doe","35"]})
        cannot exceed 10,000 bytes.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param spark_submit_params: A list of parameters for jobs with spark submit task,
        e.g. "spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"].
        The parameters will be passed to spark-submit script as command line parameters.
        If specified upon run-now, it would overwrite the parameters specified
        in job setting.
        The json representation of this field cannot exceed 10,000 bytes.
        This field will be templated.

        ..seealso::
            https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
    :param timeout_seconds: The timeout for this run. By default a value of 0 is used
        which means to have no timeout.
        This field will be templated.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :param polling_period_seconds: Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_args: An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    :param do_xcom_push: Whether we should push run_id and run_page_url to xcom.
    """

    def execute(self, context: "Context") -> None:
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
                run_id=str(self.run_id),
                run_page_url=self.run_page_url,
                retry_limit=self.databricks_retry_limit,
                retry_delay=self.databricks_retry_delay,
                polling_period_seconds=self.polling_period_seconds,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: "Context", event: Any = None
    ) -> None:  # pylint: disable=unused-argument
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=event.get("run_id"))
            context["ti"].xcom_push(key=XCOM_RUN_PAGE_URL_KEY, value=event.get("run_page_url"))
        return None
