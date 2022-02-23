 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

*************************
Contributor's Quick Guide
*************************

.. contents:: :local:

Note to Starters
################

You can run the Astronomer Providers dev env on your machine with Docker Container

It provides a more consistent environment and allows
integration tests with a number of integrations (google, amazon, databricks etc.).
However it also requires **4GB RAM, 40GB disk space and at least 2 cores**.


Prerequisites
#############

Docker Desktop


Docker Desktop Community Edition
--------------------------------

- Install Docker Desktop on your machine, based on the Operating System.Please refer [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/) .
- Confirm that the Docker Desktop installation completed successfully.
- Configure the Docker daemon by modifying the json Docker daemon configuration file,
  navigate to Preferences > Docker Engine  on the Docker Desktop and change the JSON file as below

  ```yaml

    {
      "builder": {
        "gc": {
          "enabled": true,
          "defaultKeepStorage": "20GB"
        }
      },
      "experimental": false,
      "features": {
        "buildkit": false
      }
    }
```

- Restart Docker Desktop
- Once Docker is installed, please install Docker Compose.Please refer to [https://docs.docker.com/compose/install/](https://docs.docker.com/compose/install/)



Virtual Environment
--------------------------------
Create a local virtualenv, for example:

.. code-block:: bash

   python3 -m venv airflow-env

3. Initialize and activate the created environment:

.. code-block:: bash

   source airflow-env/bin/activate


Setup and develop using PyCharm
###############################

.. raw:: html

  <details>
    <summary>Setup and develop using PyCharm</summary>


Setting up Astronomer Providers
-------------------------------

Forking and cloning Project
~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Goto https://github.com/astronomer/astronomer-providers/ and fork the project.

2. Goto your github account's fork of astronomer-providers click on ``Code`` and copy the clone link.

3. Open your IDE or source code editor and select the option to clone the repository

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/pycharm_clone.png"
             alt="Cloning github fork to Pycharm">
      </div>


4. Paste the copied clone link in the URL field and submit.

   .. raw:: html

      <div align="center" style="padding-bottom:10px">
        <img src="images/quick_start/click_on_clone.png"
             alt="Cloning github fork to Pycharm">
      </div>


Setting up Dev Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. Open terminal and enter into virtual environment ``airflow-env`` and goto project directory

.. code-block:: bash

  $ pyenv activate airflow-env
  $ cd ~/Projects/airflow/

2. Run the following shell commands from the root of the repository:

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


3. Now you can access airflow web interface on your local machine at http://127.0.0.1:8080
with user name ``admin``and password ``admin``.



Setting up Debug
~~~~~~~~~~~~~~~~

1. Debugging an example DAG

- Add Interpreter to PyCharm pointing interpreter path to ``~/airflow-env/bin/python``, which is virtual
  environment ``airflow-env`` created earlier. For adding an Interpreter go to ``File -> Setting -> Project:
  airflow -> Python Interpreter``.

- In PyCharm IDE open the project, directory ``/dev/dags`` of local machine is by default mounted to docker
  machine when airflow is started. So any DAG file present in this directory will be picked automatically by
  scheduler running in docker machine and same can be seen on ``http://127.0.0.1:28080``.

- Copy any example DAG that you would have developed  to ``/dev/dags/``.

- Now this example DAG should be picked up by the local instance of Airflow.


Testing
~~~~~~~

All Tests are inside ./tests directory.

- Just run ``pytest filepath+filename`` to run the tests.

.. code-block:: bash

   pytest tests/google/cloud/operators/test_bigquery.py
    ============================= test session starts ==============================
    platform linux -- Python 3.9.10, pytest-7.0.1, pluggy-1.0.0
    rootdir: /home/circleci/project, configfile: setup.cfg, testpaths: tests
    plugins: anyio-3.5.0, asyncio-0.18.1
    asyncio: mode=legacy
    collected 6 items

    tests/google/cloud/operators/test_bigquery.py ......

   ======================================== 6 passed in 4.88s ========================================


Pre-commit
~~~~~~~~~~

Before committing changes to github or raising a pull request,
code needs to be checked for certain quality standards
such as spell check, code syntax, code formatting, compatibility with Apache License requirements etc. T
his set of tests are applied when you commit your code.

To avoid burden on CI infrastructure and to save time, Pre-commit hooks can be run locally before committing changes.

1. Installing required Python packages

.. code-block:: bash

  $ pip install pre-commit

2. Go to your project directory

.. code-block:: bash

  $ cd ~/Projects/airflow


3. Running pre-commit hooks

.. code-block:: bash

  $ pre-commit run --all-files
    No-tabs checker......................................................Passed
    Add license for all SQL files........................................Passed
    Add license for all other files......................................Passed
    Add license for all rst files........................................Passed
    Add license for all JS/CSS/PUML files................................Passed
    Add license for all JINJA template files.............................Passed
    Add license for all shell files......................................Passed
    Add license for all python files.....................................Passed
    Add license for all XML files........................................Passed
    Add license for all yaml files.......................................Passed
    Add license for all md files.........................................Passed
    Add license for all mermaid files....................................Passed
    Add TOC for md files.................................................Passed
    Add TOC for upgrade documentation....................................Passed
    Check hooks apply to the repository..................................Passed
    black................................................................Passed
    Check for merge conflicts............................................Passed
    Debug Statements (Python)............................................Passed
    Check builtin type constructor use...................................Passed
    Detect Private Key...................................................Passed
    Fix End of Files.....................................................Passed
    ...........................................................................

4. Running pre-commit for selected files

.. code-block:: bash

  $ pre-commit run  --files pre-commit run --files astronomer/providers/databricks/operators/databricks.py

    black.........................................................................Passed
    isort.........................................................................Passed
    flake8........................................................................Passed
    check for merge conflicts.....................................................Passed
    check toml................................................(no files to check)Skipped
    check yaml................................................(no files to check)Skipped
    debug statements (python).....................................................Passed
    fix end of files..............................................................Passed
    mixed line ending.............................................................Passed
    trim trailing whitespace......................................................Passed
    Run codespell to check for common misspellings in files.......................Passed
    Check YAML files with yamllint............................(no files to check)Skipped



5. Running specific hook for selected files

.. code-block:: bash

  $ pre-commit run black --files astronomer/providers/databricks/operators/databricks.py
    black...............................................................Passed
  $ pre-commit run flake8 --files astronomer/providers/databricks/operators/databricks.py
    Run flake8..........................................................Passed



8. Enabling Pre-commit check before push. It will run pre-commit automatically before committing and stops the commit

.. code-block:: bash

  $ cd ~/Projects/astronomer-providers
  $ pre-commit install
  $ git commit -m "Added xyz"

9. To disable Pre-commit

.. code-block:: bash

  $ cd ~/Projects/astronomer-providers
  $ pre-commit uninstall



Contribution guide
~~~~~~~~~~~~~~~~~~

- To know how to contribute to the project visit |CONTRIBUTING.rst|

.. |CONTRIBUTING.rst| raw:: html

   <a href="https://github.com/astronomer/astronomer-providers/blob/main/CONTRIBUTING.rst" target="_blank">CONTRIBUTING.rst</a>

- Following are some of important links of CONTRIBUTING.rst

  - |Workflow for a contribution|

  .. |Workflow for a contribution| raw:: html

   <a href="https://github.com/astronomer/astronomer-providers/blob/main/CONTRIBUTING.rst#contribution-workflow" target="_blank">
   Workflow for a contribution</a>



Raising Pull Request
~~~~~~~~~~~~~~~~~~~~

1. Go to your GitHub account and open your fork project and click on Branches

2. Click on ``New pull request`` button on branch from which you want to raise a pull request.

3. Add title and description as per Contributing guidelines and click on ``Create pull request``.
