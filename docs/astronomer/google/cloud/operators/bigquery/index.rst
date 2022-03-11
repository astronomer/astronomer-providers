:py:mod:`google.cloud.operators.bigquery`
=========================================

.. py:module:: google.cloud.operators.bigquery

.. autoapi-nested-parse::

   This module contains Google BigQueryAsync providers.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   google.cloud.operators.bigquery.BigQueryInsertJobOperatorAsync
   google.cloud.operators.bigquery.BigQueryCheckOperatorAsync
   google.cloud.operators.bigquery.BigQueryGetDataOperatorAsync
   google.cloud.operators.bigquery.BigQueryIntervalCheckOperatorAsync
   google.cloud.operators.bigquery.BigQueryValueCheckOperatorAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   google.cloud.operators.bigquery.BIGQUERY_JOB_DETAILS_LINK_FMT


.. py:data:: BIGQUERY_JOB_DETAILS_LINK_FMT
   :annotation: = https://console.cloud.google.com/bigquery?j={job_id}

   

.. py:class:: BigQueryInsertJobOperatorAsync(configuration, project_id = None, location = None, job_id = None, force_rerun = True, reattach_states = None, gcp_conn_id = 'google_cloud_default', delegate_to = None, impersonation_chain = None, cancel_on_kill = True, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator`, :py:obj:`airflow.models.baseoperator.BaseOperator`

   Starts a BigQuery job asynchronously, and returns job id.
   This operator works in the following way:

   - it calculates a unique hash of the job using job's configuration or uuid if ``force_rerun`` is True
   - creates ``job_id`` in form of
       ``[provided_job_id | airflow_{dag_id}_{task_id}_{exec_date}]_{uniqueness_suffix}``
   - submits a BigQuery job using the ``job_id``
   - if job with given id already exists then it tries to reattach to the job if its not done and its
       state is in ``reattach_states``. If the job is done the operator will raise ``AirflowException``.

   Using ``force_rerun`` will submit a new job every time without attaching to already existing ones.

   For job definition see here:

       https://cloud.google.com/bigquery/docs/reference/v2/jobs

   :param configuration: The configuration parameter maps directly to BigQuery's
       configuration field in the job  object. For more details see
       https://cloud.google.com/bigquery/docs/reference/v2/jobs
   :param job_id: The ID of the job. It will be suffixed with hash of job configuration
       unless ``force_rerun`` is True.
       The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or
       dashes (-). The maximum length is 1,024 characters. If not provided then uuid will
       be generated.
   :param force_rerun: If True then operator will use hash of uuid as job id suffix
   :param reattach_states: Set of BigQuery job's states in case of which we should reattach
       to the job. Should be other than final states.
   :param project_id: Google Cloud Project where the job is running
   :param location: location the job is running
   :param gcp_conn_id: The connection ID used to connect to Google Cloud.
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :param cancel_on_kill: Flag which indicates whether cancel the hook's job or not, when on_kill is called

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: BigQueryCheckOperatorAsync(*, sql, gcp_conn_id = 'google_cloud_default', bigquery_conn_id = None, use_legacy_sql = True, location = None, impersonation_chain = None, labels = None, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator`

   Performs checks against BigQuery. The ``BigQueryCheckOperator`` expects
   a sql query that will return a single row. Each value on that
   first row is evaluated using python ``bool`` casting. If any of the
   values return ``False`` the check is failed and errors out.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryCheckOperator`

   Note that Python bool casting evals the following as ``False``:

   * ``False``
   * ``0``
   * Empty string (``""``)
   * Empty list (``[]``)
   * Empty dictionary or set (``{}``)

   Given a query like ``SELECT COUNT(*) FROM foo``, it will fail only if
   the count ``== 0``. You can craft much more complex query that could,
   for instance, check that the table has the same number of rows as
   the source table upstream, or that the count of today's partition is
   greater than yesterday's partition, or that a set of metrics are less
   than 3 standard deviation for the 7 day average.

   This operator can be used as a data quality check in your pipeline, and
   depending on where you put it in your DAG, you have the choice to
   stop the critical path, preventing from
   publishing dubious data, or on the side and receive email alerts
   without stopping the progress of the DAG.

   :param sql: the sql to be executed
   :type sql: str
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param use_legacy_sql: Whether to use legacy SQL (true)
       or standard SQL (false).
   :type use_legacy_sql: bool
   :param location: The geographic location of the job. See details at:
       https://cloud.google.com/bigquery/docs/locations#specifying_your_location
   :type location: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]
   :param labels: a dictionary containing labels for the table, passed to BigQuery
   :type labels: dict

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: BigQueryGetDataOperatorAsync(*, dataset_id, table_id, max_results = 100, selected_fields = None, gcp_conn_id = 'google_cloud_default', bigquery_conn_id = None, delegate_to = None, location = None, impersonation_chain = None, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator`

   Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
   and returns data in a python list. The number of elements in the returned list will
   be equal to the number of rows fetched. Each element in the list will again be a list
   where element would represent the columns values for that row.

   **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``

   .. note::
       If you pass fields to ``selected_fields`` which are in different order than the
       order of columns already in
       BQ table, the data will still be in the order of BQ table.
       For example if the BQ table has 3 columns as
       ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
       the data would still be of the form ``'A,B'``.

   **Example**: ::

       get_data = BigQueryGetDataOperator(
           task_id='get_data_from_bq',
           dataset_id='test_dataset',
           table_id='Transaction_partitions',
           max_results=100,
           selected_fields='DATE',
           gcp_conn_id='airflow-conn-id'
       )

   :param dataset_id: The dataset ID of the requested table. (templated)
   :param table_id: The table ID of the requested table. (templated)
   :param max_results: The maximum number of records (rows) to be fetched from the table. (templated)
   :param selected_fields: List of fields to return (comma-separated). If
       unspecified, all fields are returned.
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :param location: The location used for the operation.
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).

   .. py:method:: generate_query(self)

      Generate a select query if selected fields are given or with *
      for the given dataset and table id


   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: BigQueryIntervalCheckOperatorAsync(*, table, metrics_thresholds, date_filter_column = 'ds', days_back = -7, gcp_conn_id = 'google_cloud_default', bigquery_conn_id = None, use_legacy_sql = True, location = None, impersonation_chain = None, labels = None, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator`

   Checks asynchronously that the values of metrics given as SQL expressions are within
   a certain tolerance of the ones from days_back before.

   This method constructs a query like so ::
       SELECT {metrics_threshold_dict_key} FROM {table}
       WHERE {date_filter_column}=<date>

   :param table: the table name
   :param days_back: number of days between ds and the ds we want to check
       against. Defaults to 7 days
   :param metrics_thresholds: a dictionary of ratios indexed by metrics, for
       example 'COUNT(*)': 1.5 would require a 50 percent or less difference
       between the current day, and the prior days_back.
   :param use_legacy_sql: Whether to use legacy SQL (true)
       or standard SQL (false).
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :param location: The geographic location of the job. See details at:
       https://cloud.google.com/bigquery/docs/locations#specifying_your_location
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :param labels: a dictionary containing labels for the table, passed to BigQuery

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



.. py:class:: BigQueryValueCheckOperatorAsync(*, sql, pass_value, tolerance = None, gcp_conn_id = 'google_cloud_default', bigquery_conn_id = None, use_legacy_sql = True, location = None, impersonation_chain = None, labels = None, **kwargs)

   Bases: :py:obj:`airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator`

   Performs a simple value check using sql code.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:BigQueryValueCheckOperator`

   :param sql: the sql to be executed
   :type sql: str
   :param use_legacy_sql: Whether to use legacy SQL (true)
       or standard SQL (false).
   :type use_legacy_sql: bool
   :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
   :type gcp_conn_id: str
   :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
       This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
   :type bigquery_conn_id: str
   :param location: The geographic location of the job. See details at:
       https://cloud.google.com/bigquery/docs/locations#specifying_your_location
   :type location: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account (templated).
   :type impersonation_chain: Union[str, Sequence[str]]
   :param labels: a dictionary containing labels for the table, passed to BigQuery
   :type labels: dict

   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



