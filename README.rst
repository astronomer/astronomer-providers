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
.. image:: https://codecov.io/gh/astronomer/astronomer-providers/branch/main/graph/badge.svg?token=LPHFRC3CB3
    :target: https://codecov.io/gh/astronomer/astronomer-providers
    :alt: CodeCov
.. image:: https://readthedocs.org/projects/astronomer-providers/badge/?version=latest
    :target: https://astronomer-providers.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://img.shields.io/badge/security-bandit-green.svg
   :target: https://github.com/PyCQA/bandit
   :alt: Security: bandit

`Apache Airflow <https://airflow.apache.org/>`_ Providers containing Deferrable Operators & Sensors from Astronomer.

Installation
------------

Install and update using `pip <https://pip.pypa.io/en/stable/getting-started/>`_:

.. code-block:: bash

    pip install astronomer-providers

This only installs dependencies for core provider. To install all dependencies, run:

.. code-block:: bash

    pip install 'astronomer-providers[all]'

To only install the dependencies for a specific provider, specify the integration name as extra argument, example
to install Kubernetes provider dependencies, run:

.. code-block:: bash

    pip install 'astronomer-providers[cncf.kubernetes]'

Extras
^^^^^^

.. EXTRA_DOC_START

.. list-table::
   :header-rows: 1

   * - Extra Name
     - Installation Command
     - Dependencies

   * - ``all``
     - ``pip install 'astronomer-providers[all]'``
     - All

   * - ``amazon``
     - ``pip install 'astronomer-providers[amazon]'``
     - Amazon

   * - ``apache.hive``
     - ``pip install 'astronomer-providers[apache.hive]'``
     - Apache Hive

   * - ``apache.livy``
     - ``pip install 'astronomer-providers[apache.livy]'``
     - Apache Livy

   * - ``cncf.kubernetes``
     - ``pip install 'astronomer-providers[cncf.kubernetes]'``
     - Cncf Kubernetes

   * - ``databricks``
     - ``pip install 'astronomer-providers[databricks]'``
     - Databricks

   * - ``google``
     - ``pip install 'astronomer-providers[google]'``
     - Google

   * - ``microsoft.azure``
     - ``pip install 'astronomer-providers[microsoft.azure]'``
     - Microsoft Azure

   * - ``snowflake``
     - ``pip install 'astronomer-providers[snowflake]'``
     - Snowflake

.. EXTRA_DOC_END

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

**Example DAGs** for each provider is within the respective provider's folder. For example,
the Kubernetes provider's DAGs are within the
`astronomer/providers/cncf/kubernetes/example_dags <https://github.com/astronomer/astronomer-providers/tree/main/astronomer/providers/cncf/kubernetes/example_dags>`_
folder.

Principle
---------

We will only create Async operators for the "sync-version" of operators that do some level of polling
(take more than a few seconds to complete).

For example, we won’t create an async Operator for a ``BigQueryCreateEmptyTableOperator`` but will create one
for ``BigQueryInsertJobOperator`` that actually runs queries and can take hours in the worst case for task completion.

Changelog
---------

We follow `Semantic Versioning <https://semver.org/>`_ for releases.
Check `CHANGELOG.rst <https://github.com/astronomer/astronomer-providers/blob/main/CHANGELOG.rst>`_
for the latest changes.

Contributing Guide
------------------

All contributions, bug reports, bug fixes, documentation improvements, enhancements, and ideas are welcome.

A detailed overview on how to contribute can be found in the
`Contributing Guide <https://github.com/astronomer/astronomer-providers/blob/main/CONTRIBUTING.rst>`_.

As contributors and maintainers to this project, you are expected to abide by the
`Contributor Code of Conduct <https://github.com/astronomer/astronomer-providers/blob/main/CODE_OF_CONDUCT.md>`_.

Goals for the project
---------------------

- Our focus is on the speed of iteration and development in this stage of the project and so we want to be able to
  quickly iterate with our community members and customers and cut releases as necessary
- Airflow Providers are separate packages from the core ``apache-airflow`` package and we would like to avoid
  further bloating the Airflow repo
- We want users and the community to be able to easily track features and the roadmap for individual providers
  that we develop
- We would love to see the Airflow community members create, maintain and share their providers to build an Ecosystem
  of Providers.

License
-------

`Apache License 2.0 <LICENSE>`_
