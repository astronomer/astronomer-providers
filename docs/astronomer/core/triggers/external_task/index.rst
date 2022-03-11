:py:mod:`core.triggers.external_task`
=====================================

.. py:module:: core.triggers.external_task


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   core.triggers.external_task.TaskStateTrigger
   core.triggers.external_task.DagStateTrigger




.. py:class:: TaskStateTrigger(dag_id, task_id, states, execution_dates, poll_interval = 5.0)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes TaskStateTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Checks periodically in the database to see if the task exists, and has
      hit one of the states yet, or not.


   .. py:method:: count_tasks(self, session)

      Count how many task instances in the database match our criteria.



.. py:class:: DagStateTrigger(dag_id, states, execution_dates, poll_interval = 5.0)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all triggers.

   A trigger has two contexts it can exist in:

    - Inside an Operator, when it's passed to TaskDeferred
    - Actively running in a trigger worker

   We use the same class for both situations, and rely on all Trigger classes
   to be able to return the (Airflow-JSON-encodable) arguments that will
   let them be re-instantiated elsewhere.

   .. py:method:: serialize(self)

      Serializes DagStateTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Checks periodically in the database to see if the dag run exists, and has
      hit one of the states yet, or not.


   .. py:method:: count_dags(self, session)

      Count how many dag runs in the database match our criteria.



