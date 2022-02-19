Astronomer Providers
====================

Airflow Providers containing Deferrable Operators & Sensors from Astronomer.

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
