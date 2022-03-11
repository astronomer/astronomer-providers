:py:mod:`core.sensors.external_task`
====================================

.. py:module:: core.sensors.external_task


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   core.sensors.external_task.ExternalTaskSensorAsync




.. py:class:: ExternalTaskSensorAsync(*, external_dag_id, external_task_id = None, external_task_ids = None, allowed_states = None, failed_states = None, execution_delta = None, execution_date_fn = None, check_existence = False, **kwargs)

   Bases: :py:obj:`airflow.sensors.external_task.ExternalTaskSensor`

   Waits for a different DAG or a task in a different DAG to complete for a
   specific execution_date

   :param external_dag_id: The dag_id that contains the task you want to
       wait for
   :type external_dag_id: str
   :param external_task_id: The task_id that contains the task you want to
       wait for. If ``None`` (default value) the sensor waits for the DAG
   :type external_task_id: str or None
   :param external_task_ids: The list of task_ids that you want to wait for.
       If ``None`` (default value) the sensor waits for the DAG. Either
       external_task_id or external_task_ids can be passed to
       ExternalTaskSensor, but not both.
   :type external_task_ids: Iterable of task_ids or None, default is None
   :param allowed_states: Iterable of allowed states, default is ``['success']``
   :type allowed_states: Iterable
   :param failed_states: Iterable of failed or dis-allowed states, default is ``None``
   :type failed_states: Iterable
   :param execution_delta: time difference with the previous execution to
       look at, the default is the same execution_date as the current task or DAG.
       For yesterday, use [positive!] datetime.timedelta(days=1). Either
       execution_delta or execution_date_fn can be passed to
       ExternalTaskSensor, but not both.
   :type execution_delta: Optional[datetime.timedelta]
   :param execution_date_fn: function that receives the current execution date as the first
       positional argument and optionally any number of keyword arguments available in the
       context dictionary, and returns the desired execution dates to query.
       Either execution_delta or execution_date_fn can be passed to ExternalTaskSensor,
       but not both.
   :type execution_date_fn: Optional[Callable]
   :param check_existence: Set to ``True`` to check if the external task exists (when
       external_task_id is not None) or check if the DAG to wait for exists (when
       external_task_id is None), and immediately cease waiting if the external task
       or DAG does not exist (default value: False).
   :type check_existence: bool

   .. py:method:: execute(self, context)

      Correctly identify which trigger to execute, and defer execution as expected.


   .. py:method:: execute_complete(self, context, session, event = None)

      Verifies that there is a success status for each task via execution date.


   .. py:method:: get_execution_dates(self, context)

      Helper function to set execution dates depending on which context and/or internal fields are populated.
