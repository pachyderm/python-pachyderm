
Changelog
=========

Unreleased
----------

* Added: Test automation tools pytest, tox, and Travis CI.
* Added: Minikube and Pachyderm deployment to Travis CI config.
* Fixed: Python 2.7 incompability issues.
* Changed: Switched docs to reStructuredText.
* Added: Separate CHANGELOG file.
* Added: PfsClient tests for init, list_repo, create_repo, delete_repo, start_commit, finish_commit, and commit methods.
* Changed: Modified PfsClient() delete_repo method error handling to match Go client behavior.
* Changed: Modified PfsClient() start_commit and commit methods to make the branch argument optional.
* Added: Bumpversion for tagging releases and semantic versioning.
* Fixed: PfsClient() initialization ignored pachd host and port environment variables.

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
