 .. Copyright 2022 Astronomer Inc

 .. Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Astronomer Operators
====================

Closed-source Airflow operators for Astronomer customers.

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
