:py:mod:`core.triggers.filesystem`
==================================

.. py:module:: core.triggers.filesystem


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   core.triggers.filesystem.FileTrigger




Attributes
~~~~~~~~~~

.. autoapisummary::

   core.triggers.filesystem.log


.. py:data:: log




.. py:class:: FileTrigger(filepath, recursive = False, poll_interval = 5.0)

   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   A trigger that fires exactly once after it finds the requested file or folder.

   :param filepath: File or folder name (relative to the base path set within the connection), can
       be a glob.
   :type filepath: str
   :param recursive: when set to ``True``, enables recursive directory matching behavior of
       ``**`` in glob filepath parameter. Defaults to ``False``.
   :type recursive: bool

   .. py:method:: serialize(self)

      Serializes FileTrigger arguments and classpath.


   .. py:method:: run(self)
      :async:

      Simple loop until the relevant files are found.
