PyPachy
=======

A python client wrapper for *Pachyderm* <https://www.pachyderm.io/> API.

*NOTES*

1. Currently implements only the PFS interface.

2. Supports Pachyderm versions 1.4 and up


Installing
----------

```bash
    $ pip install pypachy
```

Instructions
------------
All of the PFS functions used in ``pachctl`` are supported (almost) as-is.
There are some helper functions that help make things more pythonic:
* ``commit`` which is a context manager wrapper for ``start_commit`` and ``finish_commit``
* ``get_files`` which supports getting the data from multiple files

Naming commits
--------------

All functions that accept a ``commit`` argument will accept a tuple of ``(repo, branch)`` or ``(repo, commit_id)``,
a string like ``repo/branch`` or ``repo/commit_id`` and a Commit object.

e.g:

```python
    >>> client.list_file(('my_repo', 'branch'), '/')    # tuple
    >>> client.list_file('my_repo/commit_id', '/')      # string
    >>> c = client.list_commit('my_repo')[0]            # get some commit
    >>> client.list_file(c, '/')                        # and use it directly
```


Basic usage example
-------------------

```python
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
```

TODO
----
* Test, test, test!
* Add support for ``ObjectAPI``
* Add support for ``PPS``, ``Version``, etc


Changelog
---------
``0.1.3``
- Fixed: ``inspect_commit`` was broken
- Added: ``provenances_for_repo`` function gives all the provenances for the commits in the repo

``0.1.2``
- Added: ``PfsClient`` default parameters now use the environment variables for pachd
- Added: ``put_file_bytes`` can accept an iterator
- Changed: ``commit`` now tries to close the commit if an error occurred inside the context
- Added: More examples and a changelog to README