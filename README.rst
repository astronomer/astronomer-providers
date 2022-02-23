Astronomer Providers
====================

.. image:: https://badge.fury.io/py/astronomer-providers.svg
    :target: https://badge.fury.io/py/astronomer-providers
    :alt: PyPI Version
.. image:: https://img.shields.io/pypi/pyversions/astronomer-providers
    :target: https://img.shields.io/pypi/pyversions/astronomer-providers
    :alt: PyPI - Python Version
.. image:: https://img.shields.io/pypi/l/astronomer-providers?color=blue
    :target: https://img.shields.io/pypi/l/astronomer-providers?color=blue
    :alt: PyPI - License
.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black
    :alt: Code style: black

`Apache Airflow <https://airflow.apache.org/>`_ Providers containing Deferrable Operators & Sensors from Astronomer.

Installation
------------

.. code-block:: bash

    pip install astronomer-providers

Example Usage
-------------

This repo is structured same as the Apache Airflow's source code, so for example
if you want to import Async operators, you can import it as follows:

.. code-block:: python

    from astronomer.providers.amazon.aws.sensors.s3 import S3KeySensorAsync as S3KeySensor

    waiting_for_s3_key = S3KeySensor(
        task_id="waiting_for_s3_key",
        bucket_key="sample_key.txt",
        wildcard_match=False,
        bucket_name="sample-bucket",
    )

Contributing Guide
------------------

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.

A detailed overview on how to contribute can be found in the `Contributing Guide <CONTRIBUTING.rst>`_.

As contributors and maintainers to this project, you are expected to abide by the `Contributor Code of Conduct <CODE_OF_CONDUCT.md>`_.

License
-------

`Apache License 2.0 <LICENSE>`_
