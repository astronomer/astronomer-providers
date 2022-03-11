:py:mod:`cncf.kubernetes.operators.kubernetes_pod`
==================================================

.. py:module:: cncf.kubernetes.operators.kubernetes_pod


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperatorAsync




.. py:exception:: PodNotFoundException

   Bases: :py:obj:`airflow.exceptions.AirflowException`

   Expected pod does not exist in kube-api.


.. py:class:: KubernetesPodOperatorAsync(*, poll_interval = 5, **kwargs)

   Bases: :py:obj:`airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator`

   Async (deferring) version of KubernetesPodOperator

   :param poll_interval: interval in seconds to sleep between checking pod status

   .. py:method:: raise_for_trigger_status(event)
      :staticmethod:

      Raise exception if pod is not in expected state.


   .. py:method:: execute(self, context)

      This is the main method to derive when creating an operator.
      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.
