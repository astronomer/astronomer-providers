:py:mod:`core.sensors.filesystem`
=================================

.. py:module:: core.sensors.filesystem


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   core.sensors.filesystem.FileSensorAsync




Attributes
~~~~~~~~~~

.. autoapisummary::

   core.sensors.filesystem.log


.. py:data:: log
   

   

.. py:class:: FileSensorAsync(*, filepath, fs_conn_id='fs_default', recursive=False, **kwargs)

   Bases: :py:obj:`airflow.sensors.filesystem.FileSensor`

   Waits for a file or folder to land in a filesystem using async.

   If the path given is a directory then this sensor will only return true if
   any files exist inside it (either directly, or within a subdirectory)

   :param fs_conn_id: reference to the File (path)
   :param filepath: File or folder name (relative to the base path set within the connection), can
       be a glob.
   :param recursive: when set to ``True``, enables recursive directory matching behavior of
       ``**`` in glob filepath parameter. Defaults to ``False``.

   .. py:method:: execute(self, context)

      Airflow runs this method on the worker and defers using the trigger.


   .. py:method:: execute_complete(self, context, event)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.



