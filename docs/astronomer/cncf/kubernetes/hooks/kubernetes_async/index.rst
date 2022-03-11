:py:mod:`cncf.kubernetes.hooks.kubernetes_async`
================================================

.. py:module:: cncf.kubernetes.hooks.kubernetes_async


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   cncf.kubernetes.hooks.kubernetes_async.KubernetesHookAsync




.. py:class:: KubernetesHookAsync(conn_id = default_conn_name, client_configuration = None, cluster_context = None, config_file = None, in_cluster = None)

   Bases: :py:obj:`airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook`

   Creates Kubernetes API connection.

   - use in cluster configuration by using ``extra__kubernetes__in_cluster`` in connection
   - use custom config by providing path to the file using ``extra__kubernetes__kube_config_path``
   - use custom configuration by providing content of kubeconfig file via
       ``extra__kubernetes__kube_config`` in connection
   - use default config by providing no extras

   This hook check for configuration option in the above order. Once an option is present it will
   use this configuration.

   .. seealso::
       For more information about Kubernetes connection:
       :doc:`/connections/kubernetes`

   :param conn_id: The :ref:`kubernetes connection <howto/connection:kubernetes>`
       to Kubernetes cluster.
   :type conn_id: str

   .. py:method:: get_api_client_async(self)
      :async:

      Create an API Client object to interact with Kubernetes
