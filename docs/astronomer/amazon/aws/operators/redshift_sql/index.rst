:py:mod:`amazon.aws.operators.redshift_sql`
===========================================

.. py:module:: amazon.aws.operators.redshift_sql


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   amazon.aws.operators.redshift_sql.RedshiftSQLOperatorAsync




.. py:class:: RedshiftSQLOperatorAsync(*, poll_interval = 5, **kwargs)

   Bases: :py:obj:`airflow.providers.amazon.aws.operators.redshift_sql.RedshiftSQLOperator`

   Executes SQL Statements against an Amazon Redshift cluster

   .. py:method:: execute(self, context)

      Execute a statement against Amazon Redshift


   .. py:method:: execute_complete(self, context, event = None)

      Callback for when the trigger fires - returns immediately.
      Relies on trigger to throw an exception, otherwise it assumes execution was
      successful.
