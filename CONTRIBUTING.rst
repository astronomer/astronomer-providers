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

.. contents:: :local:

Contributions
=============

Contributions are welcome and are greatly appreciated! Every little bit helps,
and credit will always be given.

This document aims to explain the subject of contributions if you have not contributed to
any Open Source project, but it will also help people who have contributed to other projects learn about the
rules of that community.

New Contributor
---------------
If you are a new contributor, please follow the `Contributors Quick Start <https://github.com/astronomer/
astronomer-providers/blob/main/CONTRIBUTORS_QUICK_START.rst>`__ guide to get a gentle step-by-step
introduction to setting up the development environment and making your first contribution.

Report Bugs
-----------

Report bugs through `GitHub <https://github.com/astronomer/astronomer-providers/issues>`__.

Please report relevant information and preferably code that exhibits the
problem.

Fix Bugs
--------

Look through the GitHub issues for bugs. Anything is open to whoever wants to
implement it.

Implement New Features
------------------

Any unassigned feature request issue is open to whoever wants to implement it.

We've created the operators, hooks, and triggers we needed, but we've
made sure that this part of Astronomer Providers is extensible. New operators, hooks,
and triggers are very welcomed!

Improve Documentation
---------------------

Astronomer Providers could always use better documentation, whether as part of the official
docs, in docstrings, ``docs/*.rst`` or even on the web as blog posts or
articles.

Submit Feedback
---------------

The best way to send feedback is to `open an issue on GitHub <https://github.com/astronomer/astronomer-providers/issues/new/choose>`__.

If you are proposing a new feature:

-   Explain in detail how it would work.
-   Keep the scope as narrow as possible to make it easier to implement.


Contributors
------------

A contributor is anyone who wants to contribute code, documentation, tests, ideas, or anything to the
Astronomer Providers project.

Contributors are responsible for:

* Fixing bugs
* Adding features

Contribution Workflow
=====================

Typically, you start your first contribution by reviewing open tickets
at `GitHub issues <https://github.com/astronomer/astronomer-providers/issues>`__.

If you create pull-request, you don't have to create an issue first, but if you want, you can do it.
Creating an issue will allow you to collect feedback or share plans with other people.

1. Make your own `fork <https://help.github.com/en/github/getting-started-with-github/fork-a-repo>`__ of
   the Astronomer Providers `main repository <https://github.com/astronomer/astronomer-providers>`__.

2. Make the change and create a `Pull Request from your fork <https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork>`__.

Step 1: Fork the Astronomer Providers Repo
-------------------------------------------
From the `astronomer/astronomer-providers <https://github.com/astronomer/astronomer-providers>`_ repo,
`create a fork <https://help.github.com/en/github/getting-started-with-github/fork-a-repo>`_:


Step 2: Configure Your Environment
----------------------------------
You can create a Docker-based env for this repo.
The Docker env is here to maintain a consistent and common development environment so that you can
replicate CI failures locally and work on solving them locally rather by pushing to CI.

You can configure the Docker-based development environment as follows:

1. Install the latest versions of the `Docker Community Edition` and `Docker Compose` and add them to the PATH.

This will mount your local sources to make them immediately visible in the environment.

2. Create a local virtualenv, for example:

.. code-block:: bash

   python3 -m venv myenv

3. Initialize and activate the created environment:

.. code-block:: bash

   source myenv/bin/activate

4. Open your IDE (for example, PyCharm) and select the virtualenv you created
   as the project's default virtualenv in your IDE.

5. Install pre-commit framework with `pip install pre-commit`

6. Run pre-commit install with `pre-commit install` to ensure that pre-commit hooks are executed
   on every commit

7. Run the following commands from the root of the repository:

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

Step 3: Prepare PR
------------------

1. Update the local sources to address the issue you are working on.

   For example, to address this example issue, do the following:

   * Find the class you should modify.

   * Find the test class where you should add tests.

   * Make sure your fork's main is synced with Astronomer Provider's main before you create a branch. See
     `How to sync your fork <#how-to-sync-your-fork>`_ for details.

   * Create a local branch for your development. Make sure to use latest
     ``astronomer-providers/main`` as base for the branch. This allows you to easily compare
     changes, have several changes that you work on at the same time and many more.

   * Modify the class and add necessary code and unit tests.

   * Run the unit tests from the IDE or local virtualenv  as you see fit.

   * Ensure test coverage is above 90% for each of the files that you are changing.

   * Run and fix all the static checks. If you have
     pre-commits installed, this step is automatically run while you are committing your code.
     If not, you can do it manually via ``git add`` and then ``pre-commit run``.

2. Rebase your fork, squash commits, and resolve all conflicts. See `How to rebase PR <#how-to-rebase-pr>`_
   if you need help with rebasing your change. Remember to rebase often if your PR takes a lot of time to
   review/fix. This will make rebase process much easier and less painful and the more often you do it,
   the more comfortable you will feel doing it.

3. Re-run static code checks again.

4. Make sure your commit has a good title and description of the context of your change, enough
   for the committer reviewing it to understand why you are proposing a change. Make sure to follow other
   PR guidelines described in `pull request guidelines <#pull-request-guidelines>`_.
   Create Pull Request!


Step 4: Pass PR Review
----------------------

You need to have review of at least one committer (if you are committer yourself, it has to be
another committer). Ideally you should have more than 1 committer reviewing the code.


Pull Request Guidelines
=======================

Before you submit a pull request (PR) from your forked repo, check that it meets
these guidelines:

-   Include tests, either as doctests, unit tests, or both, to your pull
    request.
    It will help you make sure you do not break the build with your PR and that you help increase coverage.

-   `Rebase your fork <http://stackoverflow.com/a/7244456/1110993>`__, and resolve all conflicts.

-   When merging PRs, Committer will use **Squash and Merge** which means then your PR will be merged as one commit,
    regardless of the number of commits in your PR.
    During the review cycle, you can keep a commit history for easier review, but if you need to,
    you can also squash all commits to reduce the maintenance burden during rebase.

-   Add an `Apache License <http://www.apache.org/legal/src-headers.html>`__ header
    to all new files.

    If you have `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__ enabled, they automatically add
    license headers during commit.

-   If your pull request adds functionality, make sure to update the docs as part
    of the same PR. Doc string is often sufficient. Make sure to follow the
    Sphinx compatible standards.

-   Make sure your code fulfills all the
    `static code checks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__ we have in our code. The easiest way
    to make sure of that is to use `pre-commit hooks <STATIC_CODE_CHECKS.rst#pre-commit-hooks>`__

-   Run tests locally before opening PR.

-   You can use any supported python version to run the tests, but the best is to check
    if it works for the oldest supported version (Python 3.9 currently). In rare cases
    tests might fail with the oldest version when you use features that are available in newer Python
    versions.

-   Adhere to guidelines for commit messages described in this `article <http://chris.beams.io/posts/git-commit/>`__.
    This makes the lives of those who come after you a lot easier.

Astronomer Providers Git Branches
=================================

All new development in Astronomer Providers happens in the ``main`` branch. All PRs should target that branch.

The production images are released in DockerHub from:

* main branch for development


Documentation for the Astronomer providers
-------------------------------------------------

When you are developing a provider, you are supposed to make sure it is well tested
and documented.

A well documented provider contains:

* references to packages, API used and example dags
* configuration reference
* class documentation generated from PyDoc in the code
* example dags
* how-to guides

Part of the documentation are example dags. We are using the example dags for various purposes in
providers:

* showing real examples of how your provider classes (Operators/Sensors/Transfers) can be used
* snippets of the examples are embedded in the documentation via ``exampleinclude::`` directive
* examples are executable as system tests

Testing the Astronomer providers
--------------------------------

We have high quality requirements when it comes to testing the Astronomer providers. We have to be sure
that we have enough coverage(more than 90%) and ways to tests for regressions before the community accepts such
providers.

* Unit tests have to be comprehensive and they should test for possible regressions and edge cases
  not only "green path"

* Integration tests where 'local' integration with a component is possible (for example tests with
  MySQL/Postgres DB/Trino/Kerberos all have integration tests which run with real, dockerized components

* System Tests which provide end-to-end testing, usually testing together several operators, sensors, by
  connecting to a real external system


Coding style and best practices
===============================

Most of our coding style rules are enforced programmatically by flake8 and mypy (which are run automatically
on every pull request), but there are some rules that are not yet automated and are more
semantic than style

Don't Use Asserts Outside Tests
-------------------------------

Our community agreed that to various reasons we do not use ``assert`` in production code of Astronomer Providers.

In other words instead of doing:

.. code-block:: python

    assert some_predicate()

you should do:

.. code-block:: python

    if not some_predicate():
        handle_the_case()

The one exception to this is if you need to make an assert for typechecking (which should be almost a last resort) you can do this:

.. code-block:: python

    if TYPE_CHECKING:
        assert isinstance(x, MyClass)


Database Session Handling
-------------------------

**Explicit is better than implicit.** If a function accepts a ``session`` parameter it should not commit the
transaction itself. Session management is up to the caller.

To make this easier, there is the ``create_session`` helper:

.. code-block:: python

    from sqlalchemy.orm import Session

    from airflow.utils.session import create_session


    def my_call(*args, session: Session):
        ...
        # You MUST not commit the session here.


    with create_session() as session:
        my_call(*args, session=session)

If this function is designed to be called by "end-users" (i.e. DAG authors) then using the ``@provide_session`` wrapper is okay:

.. code-block:: python

    from sqlalchemy.orm import Session

    from airflow.utils.session import NEW_SESSION, provide_session


    @provide_session
    def my_method(arg, *, session: Session = NEW_SESSION):
        ...
        # You SHOULD not commit the session here. The wrapper will take care of commit()/rollback() if exception

In both cases, the ``session`` argument is a `keyword-only argument`_. This is the most preferred form if
possible, although there are some exceptions in the code base where this cannot be used, due to backward
compatibility considerations. In most cases, ``session`` argument should be last in the argument list.

.. _`keyword-only argument`: https://www.python.org/dev/peps/pep-3102/


Don't use time() for duration calculations
-----------------------------------------

If you wish to compute the time difference between two events with in the same process, use
``time.monotonic()``, not ``time.time()`` nor ``timezone.utcnow()``.

If you are measuring duration for performance reasons, then ``time.perf_counter()`` should be used. (On many
platforms, this uses the same underlying clock mechanism as monotonic, but ``perf_counter`` is guaranteed to be
the highest accuracy clock on the system, monotonic is simply "guaranteed" to not go backwards.)

If you wish to time how long a block of code takes, use ``Stats.timer()`` -- either with a metric name, which
will be timed and submitted automatically:

.. code-block:: python

    from airflow.stats import Stats

    ...

    with Stats.timer("my_timer_metric"):
        ...

or to time but not send a metric:

.. code-block:: python

    from airflow.stats import Stats

    ...

    with Stats.timer() as timer:
        ...

    log.info("Code took %.3f seconds", timer.duration)

For full docs on ``timer()`` check out `airflow/stats.py`_.

If the start_date of a duration calculation needs to be stored in a database, then this has to be done using
datetime objects. In all other cases, using datetime for duration calculation MUST be avoided as creating and
diffing datetime operations are (comparatively) slow.

Naming Conventions for provider packages
----------------------------------------

* Provider packages are all placed in 'astronomer.providers'

* Providers are usually direct sub-packages of the 'astronomer.providers' package but in some cases they can be
  further split into sub-packages. This is the case when the providers are connected under common umbrella but
  very loosely coupled on the code level.

* In some cases the package can have sub-packages but they are all delivered as single provider
  package (for example 'google' package may contains 'ads', 'cloud' etc. sub-packages). This is in case
  the providers are connected under common umbrella and they are also tightly coupled on the code level.

* Typical structure of provider package:
    * example_dags -> example DAGs are stored here (used for documentation and System Tests)
    * hooks -> hooks are stored here
    * operators -> operators are stored here
    * triggers -> triggers are stored here
    * sensors -> sensors are stored here

* Module names do not contain word "hooks", "operators" etc. The right type comes from
  the package. For example 'hooks.databricks' module contains DatabricksHookAsync and 'operators.databricks'
  contains DatabricksSubmitRunOperatorAsync operator.

* Class names contain 'Operator', 'Hook', 'Sensor', 'Trigger' - for example BigQueryInsertJobOperatorAsync, BigQueryHookAsync

* Operator name usually follows the convention: ``<Subject><Action><Entity>OperatorAsync``
  (BigQueryInsertJobOperatorAsync) is a good example

* Tests are grouped in parallel packages under "tests" top level package. Module name is usually
  ``test_<object_to_test>.py``,

* System test DAGs (not yet fully automated but allowing to run e2e testing of particular provider) are
  placed under example_dags folder within respective provider.

Test Infrastructure
===================

We support the following types of tests:

* **Unit tests** are Python tests launched with ``pytest``.
  Unit tests are available in the tests package

* **System tests** are automatic tests that use external systems like
  Google Cloud. These tests are intended for an end-to-end DAG execution.


How to sync your fork
=====================

When you have your fork, you should periodically synchronize the main of your fork with the
Astronomer Providers main. In order to do that you can ``git pull --rebase`` to your local git repository from
apache remote and push the main (often with ``--force`` to your fork). There is also an easy
way to sync your fork in GitHub's web UI with the `Fetch upstream feature
<https://docs.github.com/en/github/collaborating-with-pull-requests/working-with-forks/syncing-a-fork#syncing-a-fork-from-the-web-ui>`_.

This will force-push the ``main`` branch from ``astronomer/astronomer-providers`` to the ``main`` branch
in your fork. Note that in case you modified the main in your fork, you might loose those changes.


Commit Policy
=============

* Commits need a +1 vote from a committer who is not the author
* Do not merge a PR that regresses linting or does not pass CI tests (unless we have
  justification such as clearly transient error).
