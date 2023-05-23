How to release?
===============

Create a new release branch from main
-------------------------------------

Create a new release branch from ``main`` branch with the name ``release-<version>``.
e.g. If you want to release version ``1.17.6``, you can create a new branch called ``release-1-17-6`` cut from ``main`` branch.

Note: It is important to prefix your release branch name with ``release-``. This is because we run a CircleCI job to generate 
the constraints files only on such branches and the ``main`` branch.

Decide on the new version number
--------------------------------

You need to increment the version number according to `SemVer <https://semver.org/>`_. The rules for
incrementing are as follows:

* Major: Backwards-incompatible / breaking changes, or headline new features.
* Minor: Changes that do not require DAG changes and adds new features like adding
  new Operators/Sensors/Hooks to the package.
* Patch: Anything else, mainly bug-fixes and security fixes.

**Minor** and **Patch** versions should not contain any breaking changes.

Update the version number
-------------------------

You will need to update it in:

* ``setup.cfg`` - in ``version`` key of ``[metadata]`` section
* ``docs/conf.py`` - ``release`` variable
* ``astronomer/providers/package.py`` - ``get_provider_info`` method return value ``versions``

You can also use `commitizen <https://github.com/commitizen-tools/commitizen>`_ to update the version in these files

Install commitizen on mac

```
brew install commitizen
```

Bump versions locally

```
make ASTRO_PROVIDER_VERSION=<RELEASE_VERSION> bump-version
```

Note: ```RELEASE_VERSION``` is the software version you want to release.

Compare the commits introduced since the last release to aid building the CHANGELOG
-----------------------------------------------------------------------------------

You can use the following link to compare the commits introduced since the last release (e.g. 1.15.4)

```https://github.com/astronomer/astronomer-providers/compare/1.15.4...main```

Note: Make sure to replace the last release version in the above URL

Write the changelog
-------------------

Add the new release to ``CHANGELOG.rst``, along with as many release notes
as you can (more information is always better than less, though try to group
it with larger changes at the top).


Commit the release
------------------

Bundle up these changes into a single commit with the message in the format
"Release 1.2.1". Submit a pull request for this commit and wait for approval
unless you are releasing an urgent security fix.


Tag and push the commit
-----------------------

Once the release branch has been approved and merged to ``main``, checkout to the ``main`` branch.
Tag that commit with a tag that matches the version number (``git tag 1.2.1 <latest commit sha>``),
and then push the tag (``git push origin 1.2.1``).

CircleCI will handle the rest - building, testing, and pushing the resulting
artifacts to PyPI. Keep an eye out for test failures, though.

Create a new release on GitHub
------------------------------

Create a new release on GitHub by clicking on
`this link <https://github.com/astronomer/astronomer-providers/releases/new>`_.
Choose the tag you just created, and fill in the release notes or click on Auto-generate
release notes.

Use `1.1.0 release notes <https://github.com/astronomer/astronomer-providers/releases/tag/1.1.0>`_
as an example.

Verify Docs are Published
-------------------------

Verify that new docs are published on `ReadTheDocs <https://astronomer-providers.readthedocs.io/>`_ site.
The ``stable`` version of the docs should be the same as the version number of the release.
You can verify this checking the changelog entries.

Close the Milestone
-------------------

Close the milestone on GitHub for that particular version.
Example: https://github.com/astronomer/astronomer-providers/milestone/2

Make an announcement to the releases channel on Astronomer Slack
----------------------------------------------------------------

Make an announcement to the releases channel on Astronomer Slack by referring to the template from one of the previous releases.:

Create Stable Branch
--------------------

If you are releasing a new *major* release, then you should make a new branch
called ``x-0-stable`` for the previous release from the last commit on
``main`` for that release.

Bump the astronomer-providers version for new development
---------------------------------------------------------

Create a PR to bump the ``astronomer-providers`` version to the next minor dev version.e.g. If you just released ``1.15.6``, then
the next version should be ``1.16.0-dev1``. This PR should be merged to ``main`` branch.
