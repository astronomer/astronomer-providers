Available Operators and Sensors
-------------------------------

Since ``astronomer-providers>=1.19.0``, most of the operators and sensors are now deprecated and their instantiations
are proxied to their upstream Apache Airflow providers' deferrable counterparts.
Please check the deprecation status in the ``Deprecated`` column in the tables below. If the status is ``Yes``,
then you're suggested to use the replacement suggestion provided as ``New Path`` in the ``Import path`` column and
pass the ``deferrable=True`` param to the operator/sensor instantiation in your DAG.

.. traverse_operators_sensors::
  :hidden:
  :maxdepth: 2
