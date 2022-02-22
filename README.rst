Astronomer Providers
====================

.. image:: https://badge.fury.io/py/astronomer-providers.svg
    :target: https://badge.fury.io/py/astronomer-providers
    :alt: PyPI Version
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

Development Environment
------------------------

Run the following commands from the root of the repository:

- ``make dev`` - To create a development Environment using `docker-compose` file.
- ``make logs`` - To view the logs of the all the containers
- ``make stop`` - To stop all the containers
- ``make clean`` - To remove all the containers along with volumes
- ``make help`` - To view the available commands
- ``make build-run`` - To build the docker image and then run containers
- ``make restart`` - To restart Scheduler & Triggerer containers
- ``make restart-all`` - To restart all the containers
- ``make run-tests`` - Run CI tests
- ``make run-static-checks`` - Run CI static code checks

Following ports are accessible from the host machine:

- ``8080`` - Webserver
- ``5555`` - Flower
- ``5432`` - Postgres

Dev Directories:

- ``dev/dags/`` - DAG Files
- ``dev/logs`` - Logs files of the Airflow containers

License
-------

`Apache License 2.0 <LICENSE>`_
