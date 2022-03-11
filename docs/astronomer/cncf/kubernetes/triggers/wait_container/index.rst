:py:mod:`cncf.kubernetes.triggers.wait_container`
=================================================

.. py:module:: cncf.kubernetes.triggers.wait_container


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cncf.kubernetes.triggers.wait_container.WaitContainerTrigger




.. py:exception:: PodLaunchTimeoutException

   Bases: :py:obj:`airflow.exceptions.AirflowException`

   When pod does not leave the ``Pending`` phase within specified timeout.


.. py:class:: WaitContainerTrigger(*, container_name, pod_name, pod_namespace, kubernetes_conn_id = None, hook_params = None, pending_phase_timeout = 120, poll_interval = 5)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   First, waits for pod ``pod_name`` to reach running state within ``pending_phase_timeout``.
   Next, waits for ``container_name`` to reach a terminal state.

   :param kubernetes_conn_id: Airflow connection ID to use
   :param hook_params: kwargs for hook
   :param container_name: container to wait for
   :param pod_name: name of pod to monitor
   :param pod_namespace: pod namespace
   :param pending_phase_timeout: max time in seconds to wait for pod to leave pending phase
   :param poll_interval: number of seconds between reading pod state


   .. py:method:: serialize(self)

      Returns the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: get_hook(self)
      :async:


   .. py:method:: wait_for_pod_start(self, v1_api)
      :async:

      Loops until pod phase leaves ``PENDING``
      If timeout is reached, throws error.


   .. py:method:: wait_for_container_completion(self, v1_api)
      :async:

      Waits until container ``self.container_name`` is no longer in running state.


   .. py:method:: run(self)
      :async:

      Runs the trigger in an asynchronous context.

      The trigger should yield an Event whenever it wants to fire off
      an event, and return None if it is finished. Single-event triggers
      should thus yield and then immediately return.

      If it yields, it is likely that it will be resumed very quickly,
      but it may not be (e.g. if the workload is being moved to another
      triggerer process, or a multi-event trigger was being used for a
      single-event task defer).

      In either case, Trigger classes should assume they will be persisted,
      and then rely on cleanup() being called when they are no longer needed.
