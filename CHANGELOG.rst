
Changelog
=========

Unreleased
----------

* Added: Test automation tools pytest, tox, and Travis CI.
* Added: Minikube and Pachyderm deployment to Travis CI.
* Fixed: Python 2.7 incompability issues.
* Changed: Switched docs to reStructuredText.
* Added: Separate CHANGELOG file.
* Added: PfsClient tests for init, list_repo, create_repo, and delete_repo.
* Changed: Modified PfsClient() delete_repo() method error handling to match Go client behavior.
* Added: Bumpversion for tagging releases and semantic versioning.

0.1.5 (2017-08-06)
------------------

* Fixed: Miscellaneous bugs.

0.1.4 (2017-08-06)
------------------

* Added: Alpha support for PPS.
* Changed: Adapted for Pachyderm ``1.5.2``.

0.1.3 (2017-05-18)
------------------

* Fixed: ``inspect_commit`` was broken.
* Added: ``provenances_for_repo`` function gives all the provenances for the commits in the repo.

0.1.2 (2017-04-20)
------------------

* Added: ``PfsClient`` default parameters now use the environment variables for pachd.
* Added: ``put_file_bytes`` can accept an iterator.
* Changed: ``commit`` now tries to close the commit if an error occurred inside the context.
* Added: More examples and a changelog to README.
