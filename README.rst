========
Python Pachyderm
========

Official Python Pachyderm Client

Created by [kalugny](https://github.com/kalugny)

(Formerly kalugny/pypachy)

.. start-badges


.. image:: https://img.shields.io/pypi/v/pypachy.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/pypachy

.. image:: https://img.shields.io/pypi/wheel/pypachy.svg
    :alt: PyPI Wheel
    :target: https://pypi.python.org/pypi/pypachy

.. image:: https://img.shields.io/pypi/pyversions/pypachy.svg
    :alt: Supported versions
    :target: https://pypi.python.org/pypi/pypachy

.. image:: https://img.shields.io/github/commits-since/kalugny/pypachy/v0.1.5.svg
    :alt: Commits since latest release
    :target: https://github.com/kalugny/pypachy/compare/v0.1.5...master


.. end-badges

Python Pachyderm Client

A python client wrapper for the Pachyderm_ API.

*Notes*:

* Currently implements the PFS interface and only alpha support for the PPS interface.

* Supports Pachyderm versions 1.4 and up.

Installation
============

.. code:: bash

    pip install pypachy

Usage and options
=================

All of the PFS functions used in ``pachctl`` are supported (almost) as-is.

There are some helper functions that help make things more pythonic:

* ``commit`` which is a context manager wrapper for ``start_commit`` and ``finish_commit``
* ``get_files`` which supports getting the data from multiple files

Naming commits
--------------

All functions that accept a ``commit`` argument will accept a tuple of ``(repo, branch)`` or ``(repo, commit_id)``,
a string like ``repo/branch`` or ``repo/commit_id`` and a Commit object.

e.g:

.. code:: python

    >>> client.list_file(('my_repo', 'branch'), '/')    # tuple
    >>> client.list_file('my_repo/commit_id', '/')      # string
    >>> c = client.list_commit('my_repo')[0]            # get some commit
    >>> client.list_file(c, '/')                        # and use it directly

Basic usage example
-------------------

.. code:: python

    >>> import pypachy
    
    >>> client = pypachy.PfsClient()
    >>> client.create_repo('test')
    >>> with client.commit('test', 'master') as c:
    ...:     client.put_file_bytes(c, '/dir_a/data', b'DATA')
    ...:     client.put_file_url(c, '/dir_b/icon.png', 'http://www.pearl-guide.com/forum/images/smilies/biggrin.png')
    ...:
    >>> client.get_files('test/master', '/', recursive=True)
    {'/dir_a/data': b'DATA',
     '/dir_b/icon.png': b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x10\x00\x00\x00\x10\x08...'}

As of version 0.1.4, there is also limited support for PPS:

.. code:: python

    >>> pps_client = pypachy.PpsClient()
    >>> pps_client.list_pipeline()
    ...

To Do
=====

* Achieve full test coverage for PFS and PPS.
* Add support for ``description`` field in ``Commit``.
* Add support for ``ObjectAPI``

Changelog
=========

See `CHANGELOG.rst <https://github.com/kalugny/pypachy/blob/master/CHANGELOG.rst>`_.

.. _Pachyderm: https://pachyderm.io/
