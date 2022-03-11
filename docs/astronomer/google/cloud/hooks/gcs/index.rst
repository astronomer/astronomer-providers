:py:mod:`google.cloud.hooks.gcs`
================================

.. py:module:: google.cloud.hooks.gcs

.. autoapi-nested-parse::

   This module contains a Google Cloud Storage hook.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   google.cloud.hooks.gcs.GCSHookAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   google.cloud.hooks.gcs.DEFAULT_TIMEOUT


.. py:data:: DEFAULT_TIMEOUT
   :annotation: = 60



.. py:class:: GCSHookAsync(**kwargs)

   Bases: :py:obj:`astronomer.providers.google.common.hooks.base_google.GoogleBaseHookAsync`

   Abstract base class for hooks, hooks are meant as an interface to
   interact with external systems. MySqlHook, HiveHook, PigHook return
   object that can handle the connection and interaction to specific
   instances of these systems, and expose consistent methods to interact
   with them.

   .. py:attribute:: sync_hook_class




   .. py:method:: get_storage_client(self, session)
      :async:

      Returns a Google Cloud Storage service object.
