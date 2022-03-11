:py:mod:`google.common.hooks.base_google`
=========================================

.. py:module:: google.common.hooks.base_google


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   google.common.hooks.base_google.GoogleBaseHookAsync




.. py:class:: GoogleBaseHookAsync(**kwargs)

   Bases: :py:obj:`airflow.hooks.base.BaseHook`

   Abstract base class for hooks, hooks are meant as an interface to
   interact with external systems. MySqlHook, HiveHook, PigHook return
   object that can handle the connection and interaction to specific
   instances of these systems, and expose consistent methods to interact
   with them.

   .. py:attribute:: sync_hook_class
      :annotation: :Any



   .. py:method:: get_sync_hook(self)
      :async:

      Sync version of the Google Cloud Hooks makes blocking calls in ``__init__`` so we don't inherit
      from it.


   .. py:method:: service_file_as_context(self)
      :async:
