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

Issues, PRs & Discussions
-------------------------

If you have suggestions for how this project could be improved, or want to
report a bug, open an issue! We'd love all and any contributions. If you have questions, too, we'd love to hear them.

We'd also love PRs. If you're thinking of a large PR, we advise opening up an issue first to talk about it,
though! Look at the links below if you're not sure how to open a PR.

If you have other questions, use `Github Discussions <https://github.com/astronomer/astronomer-providers/discussions/>`_

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

8. If you want to add Connections, create a ``connections.yaml`` file in the ``dev`` directory.

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
   - ``make docs`` - To build the docs using Sphinx
   - ``make help`` - To view the available commands
   - ``make build`` - To re-build the docker image
   - ``make build-run`` - To build the docker image and then run containers
   - ``make restart`` - To restart Scheduler & Triggerer containers
   - ``make restart-all`` - To restart all the containers
   - ``make run-mypy`` - Run MyPy checks
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

Static code checks
------------------

We check our code quality via static code checks. The static code checks in astronomer-providers are used to verify
that the code meets certain quality standards. All the static code checks can be run through pre-commit hooks.

Your code must pass all the static code checks in the CI in order to be eligible for Code Review.
The easiest way to make sure your code is good before pushing is to use pre-commit checks locally
as described in the static code checks documentation.

You can also run some static code checks via make command using available bash scripts.

.. code-block:: bash

    make run-static-checks

Pre-commit hooks
----------------

Pre-commit hooks help speed up your local development cycle and place less burden on the CI infrastructure.
Consider installing the pre-commit hooks as a necessary prerequisite.

The pre-commit hooks by default only check the files you are currently working on and make
them fast. Yet, these checks use exactly the same environment as the CI tests
use. So, you can be sure your modifications will also work for CI if they pass
pre-commit hooks.

We have integrated the fantastic `pre-commit <https://pre-commit.com>`__ framework
in our development workflow. To install and use it, you need at least Python 3.7 locally.

Installing pre-commit hooks
^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is the best to use pre-commit hooks when you have your local virtualenv or conda environment
for astronomer-providers activated since then pre-commit hooks and other dependencies are
automatically installed. You can also install the pre-commit hooks manually
using ``pip install``.

.. code-block:: bash

    pip install pre-commit

After installation, pre-commit hooks are run automatically when you commit the code and they will
only run on the files that you change during your commit, so they are usually pretty fast and do
not slow down your iteration speed on your changes. There are also ways to disable the ``pre-commits``
temporarily when you commit your code with ``--no-verify`` switch or skip certain checks that you find
to much disturbing your local workflow.

Enabling pre-commit hooks
^^^^^^^^^^^^^^^^^^^^^^^^^

To turn on pre-commit checks for ``commit`` operations in git, enter:

.. code-block:: bash

    pre-commit install


To install the checks also for ``pre-push`` operations, enter:

.. code-block:: bash

    pre-commit install -t pre-push


For details on advanced usage of the install method, use:

.. code-block:: bash

   pre-commit install --help


Coding style and best practices
-------------------------------

Most of our coding style rules are enforced programmatically by flake8 and mypy (which are run automatically
on every pull request), but there are some rules that are not yet automated and are more Airflow specific or
semantic than style.

Naming Conventions
^^^^^^^^^^^^^^^^^^

* Class names contain 'Operator', 'Hook', 'Sensor', 'Trigger' - for example ``BigQueryInsertJobOperatorAsync``, ``BigQueryHookAsync``

* Operator name usually follows the convention: ``<Subject><Action><Entity>OperatorAsync``
  (``BigQueryInsertJobOperatorAsync``) is a good example

* Tests are grouped in parallel packages under "tests" top level package. Module name is usually
  ``test_<object_to_test>.py``

* System/Example test DAGs are placed under ``example_dags`` folder within respective folders.


Guideline to write an example DAG
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- The example DAG should be self-sufficient as it is tested as part of the CI. For example, while implementing example DAG for ``S3KeySensorAsync``, the DAG should first create bucket, then upload s3 key, the check for key using ``S3KeySensorAsync`` and then finally delete the bucket once sensor found the key.
- Add proper doc-strings as part of example DAG.
- Include a long running query always in the example DAG.
- Include a clean up step at the start of the example DAG so that there won't be failures if the resources are already present.
- Run all the steps in example DAG even if a particular task fails.

Considerations while writing Async or Deferrable Operator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
- Writing a deferrable or async operator takes a bit more work. There are some main points to consider:
    - Deferrable Operators & Triggers rely on more recent asyncio features, and as a result only work on Python 3.7 or higher.
    - Any Deferrable **Operator** implementation needs the API used to give you a unique identifier in order to poll for the status in the Trigger. This does not affect creating an async Sensor as "sensors" are just poll-based whereas "Operators" are "Submit + Poll" operation.
      For example in the below code snippet, the Google BigQuery API returns a job_id using which we can track the status of the job execution from the Trigger.

    .. code-block:: python

        job = self._submit_job(hook, configuration=configuration)
        self.job_id = job.job_id
        self.defer(
            timeout=self.execution_timeout,
            trigger=BigQueryGetDataTrigger(
                conn_id=self.gcp_conn_id,
                job_id=self.job_id,
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                project_id=hook.project_id,
            ),
            method_name="execute_complete",
        )
    - Your Operator must defer itself with a Trigger. If there is a Trigger in core Airflow you can use, great; otherwise, you will have to write one.
    - Your Operator will be stopped and removed from its worker while deferred, and no state will persist automatically. You can persist state by asking Airflow to resume you at a certain method or pass certain kwargs, but that’s it.
    - You can defer multiple times, and you can defer before/after your Operator does significant work, or only defer if certain conditions are met (e.g. a system does not have an immediate answer). Deferral is entirely under your control.
    - Any Operator can defer; no special marking on its class is needed, and it’s not limited to Sensors.
- If you want to trigger deferral, at any place in your Operator you can call ``self.defer(trigger, method_name, kwargs, timeout)``, which will raise a special exception that Airflow will catch. The arguments are:
    - ``trigger``: An instance of a Trigger that you wish to defer on. It will be serialized into the database.
    - ``method_name``: The method name on your Operator you want Airflow to call when it resumes.
    - ``kwargs``: Additional keyword arguments to pass to the method when it is called. Optional, defaults to {}.
    - ``timeout``: A timedelta that specifies a timeout after which this deferral will fail, and fail the task instance. Optional, defaults to None, meaning no timeout.
- A Trigger is written as a class that inherits from ``BaseTrigger``, and implements three methods:
    - ``__init__``, to receive arguments from Operators instantiating it
    - ``run``, an asynchronous method that runs its logic and yields one or more TriggerEvent instances as an asynchronous generator
    - ``serialize``, which returns the information needed to re-construct this trigger, as a tuple of the classpath, and keyword arguments to pass to ``__init__``
- There’s also some design constraints in the Trigger to be aware of:
    - From Operator we cannot pass a class object to Trigger because ``serialize`` method will only support JSON-serializable values.
    - The ``run`` method must be asynchronous (using Python’s asyncio), and correctly ``await`` whenever it does a blocking operation.
    - ``run`` must ``yield`` its TriggerEvents, not return them. If it returns before yielding at least one event, Airflow will consider this an error and fail any Task Instances waiting on it. If it throws an exception, Airflow will also fail any dependent task instances.
    - You should assume that a trigger instance may run more than once (this can happen if a network partition occurs and Airflow re-launches a trigger on a separated machine). So you must be mindful about side effects. For example you might not want to use a trigger to insert database rows.
    - If your trigger is designed to emit more than one event (not currently supported), then each emitted event must contain a payload that can be used to deduplicate events if the trigger is being run in multiple places. If you only fire one event and don’t need to pass information back to the Operator, you can just set the payload to ``None``.
    - A trigger may be suddenly removed from one triggerer service and started on a new one, for example if subnets are changed and a network partition results, or if there is a deployment. If desired you may implement the ``cleanup`` method, which is always called after ``run`` whether the trigger exits cleanly or otherwise.
- The Async version of the operator should ideally be easily swappable and no DAG-facing changes should be required apart from changing Import Paths.
- See if the official library supports async, if not find a third-party library that supports async calls. For example, ``pip install apache-airflow-providers-snowflake`` also installs ``snowflake-connector-python`` which officially support async calls to execute the queries. So it is used directly to implement deferrable operators for Snowflake. But many providers don't come with official support for async like Amazon. If not some research to find the right third-party library that support calls is important. In case of Amazon, we use `aiobotocore <https://github.com/aio-libs/aiobotocore>`_ for Async client for amazon services using botocore and aiohttp/asyncio.
- Inheriting the sync version of the operator wherever possible so boilerplate code can be avoided while keeping consistency. And then replacing the logic of the execute method.
- Logging: Passing the Status of the task from Trigger to the Operator or Sensors so the logs show up in the Task Logs since Triggerer logs don’t make it to Task Logs

Some Common Pitfalls
--------------------
- At times the async implementation might require to call the synchronous function. We use `asgiref <https://github.com/django/asgiref>`_ ``sync_to_async`` function wrappers for this. ``sync_to_async`` lets async code call a synchronous function, which is run in a threadpool and control returned to the async coroutine when the synchronous function completes. For example:
    .. code-block:: python

        async def service_file_as_context(self) -> Any:  # noqa: D102
            sync_hook = await self.get_sync_hook()
            return await sync_to_async(sync_hook.provide_gcp_credential_file_as_context)()

- While implementing trigger serialize method, its important to use the correct class name.
    .. code-block:: python

            def serialize(self) -> Tuple[str, Dict[str, Any]]:
                """Serialize S3KeyTrigger arguments and classpath."""
                return (
                    "astronomer.providers.amazon.aws.triggers.s3.S3KeyTrigger",
                    {
                        "bucket_name": self.bucket_name,
                        "bucket_key": self.bucket_key,
                        "wildcard_match": self.wildcard_match,
                        "aws_conn_id": self.aws_conn_id,
                        "hook_params": self.hook_params,
                    },
                )

- Add the github issue-id as part of the PR request
- Write unit tests which respect the code coverage toleration
- Git commit messages aligned to open source standards
- Rebase the code from ``main`` branch regularly.

Debugging
---------

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
