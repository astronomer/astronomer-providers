Contributions
=============

Hi there! We're thrilled that you'd like to contribute to this project. Your help is essential for keeping it great.

Please note that this project is released with a `Contributor Code of Conduct <CODE_OF_CONDUCT.md>`_.
By participating in this project you agree to abide by its terms.

Principle
---------

We will only create Async operators for the "sync-version" of operators that do some level of polling
(take more than a few seconds to complete).

For example, we won’t create an async Operator for a ``BigQueryCreateEmptyTableOperator`` but will create one
for ``BigQueryInsertJobOperator`` that actually runs queries and can take hours in the worst case for task completion.

Issues and PRs
---------------

If you have suggestions for how this project could be improved, or want to
report a bug, open an issue! We'd love all and any contributions. If you have questions, too, we'd love to hear them.

We'd also love PRs. If you're thinking of a large PR, we advise opening up an issue first to talk about it,
though! Look at the links below if you're not sure how to open a PR.


Dev Environment
---------------

You can configure the Docker-based development environment as follows:

1. Install the latest versions of the **Docker Community Edition** and **Docker Compose** and add them to the ``PATH``.

2. Create a local virtualenv, for example:

.. code-block:: bash

   python3 -m venv myenv

3. Initialize and activate the created environment:

.. code-block:: bash

   source myenv/bin/activate

4. Open your IDE (for example, PyCharm) and select the virtualenv you created
   as the project's default virtualenv in your IDE.

5. Install pre-commit framework with ``pip install pre-commit``.

6. Run ``pre-commit install`` to ensure that pre-commit hooks are executed
   on every commit

7. Put the DAGs you want to run in the ``dev/dags`` directory:

8. If you want to add Connections, create a ``connections.yaml`` file in the `dev` directory.

   See the `Connections Guide <https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html>`_ for more information.

   Example:

   .. code-block:: yaml

      druid_broker_default:
        conn_type: druid
        extra: '{"endpoint": "druid/v2/sql"}'
        host: druid-broker
        login: null
        password: null
        port: 8082
        schema: null
      airflow_db:
        conn_type: mysql
        extra: null
        host: mysql
        login: root
        password: plainpassword
        port: null
        schema: airflow

9. The following commands are available to run from the root of the repository.

   - ``make dev`` - To create a development Environment using ``docker-compose`` file.
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
   - ``dev/logs/`` - Logs files of the Airflow containers

Prepare PR
----------

1. Update the local sources to address the issue you are working on.

   * Make sure your fork's main is synced with Astronomer Provider's main before you create a branch. See
     `How to sync your fork <#how-to-sync-your-fork>`_ for details.

   * Create a local branch for your development. Make sure to use latest
     ``astronomer-providers/main`` as base for the branch. This allows you to easily compare
     changes, have several changes that you work on at the same time and many more.

   * Add necessary code and unit tests.

   * Run the unit tests from the IDE or local virtualenv as you see fit.

   * Ensure test coverage is above **90%** for each of the files that you are changing.

   * Run and fix all the static checks. If you have
     pre-commits installed, this step is automatically run while you are committing your code.
     If not, you can do it manually via ``git add`` and then ``pre-commit run``.

2. Remember to keep your branches up to date with the ``main`` branch, squash commits, and
   resolve all conflicts.

3. Re-run static code checks again.

4. Make sure your commit has a good title and description of the context of your change, enough
   for the committer reviewing it to understand why you are proposing a change. Make sure to follow other
   PR guidelines described in `pull request guidelines <#pull-request-guidelines>`_.
   Create Pull Request!

Pull Request Guidelines
-----------------------

Before you submit a pull request (PR), check that it meets these guidelines:

-   Include tests unit tests and example DAGs (wherever applicable) to your pull request.
    It will help you make sure you do not break the build with your PR and that you help increase coverage.

-   `Rebase your fork <http://stackoverflow.com/a/7244456/1110993>`__, and resolve all conflicts.

-   When merging PRs, Committer will use **Squash and Merge** which means then your PR will be merged as one commit,
    regardless of the number of commits in your PR.
    During the review cycle, you can keep a commit history for easier review, but if you need to,
    you can also squash all commits to reduce the maintenance burden during rebase.

-   If your pull request adds functionality, make sure to update the docs as part
    of the same PR. Doc string is often sufficient. Make sure to follow the
    Sphinx compatible standards.

-   Run tests locally before opening PR.

-   Adhere to guidelines for commit messages described in this `article <http://chris.beams.io/posts/git-commit/>`__.
    This makes the lives of those who come after you a lot easier.

Naming Conventions
------------------

* Class names contain 'Operator', 'Hook', 'Sensor', 'Trigger' - for example ``BigQueryInsertJobOperatorAsync``, ``BigQueryHookAsync``

* Operator name usually follows the convention: ``<Subject><Action><Entity>OperatorAsync``
  (``BigQueryInsertJobOperatorAsync``) is a good example

* Tests are grouped in parallel packages under "tests" top level package. Module name is usually
  ``test_<object_to_test>.py``

* System/Example test DAGs are placed under ``example_dags`` folder within respective folders.

Setting up Debug
----------------

1. Debugging an example DAG

- Add Interpreter to PyCharm pointing interpreter path to ``~/airflow-env/bin/python``, which is virtual
  environment ``airflow-env`` created earlier. For adding an Interpreter go to ``File -> Setting -> Project:
  airflow -> Python Interpreter``.

- In PyCharm IDE open the project, directory ``/dev/dags`` of local machine is by default mounted to docker
  machine when airflow is started. So any DAG file present in this directory will be picked automatically by
  scheduler running in docker machine and same can be seen on ``http://127.0.0.1:8080``.

- Copy any example DAG that you would have developed  to ``/dev/dags/``.

- Now this example DAG should be picked up by the local instance of Airflow.

Testing
-------

All tests are inside ``./tests`` directory.

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
