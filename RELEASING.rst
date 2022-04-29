How to release?
===============

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


Write the changelog
-------------------

Add the new release to ``CHANGELOG.rst``, along with as many release notes
as you can (more information is always better than less, though try to group
it with larger changes at the top).


Commit the release
------------------

Bundle up these changes into a single commit with the message in the format
"Releasing 1.2.1". Submit a pull request for this commit and wait for approval
unless you are releasing an urgent security fix.


Tag and push the commit
-----------------------

Tag that commit with a tag that matches the version number (``git tag 1.2.1``),
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

Create Stable Branch
--------------------

If you are releasing a new *major* release, then you should make a new branch
called ``x-0-stable`` for the previous release from the last commit on
``main`` for that release.
