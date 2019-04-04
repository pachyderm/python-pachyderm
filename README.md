# Python Pachyderm

[![PyPI Package latest releasee](https://img.shields.io/pypi/v/python-pachyderm.svg)](https://pypi.python.org/pypi/python-pachyderm)
[![Supported versions](https://img.shields.io/pypi/pyversions/python-pachyderm.svg)](https://pypi.python.org/pypi/python-pachyderm)
[![Slack Status](http://slack.pachyderm.io/badge.svg)](http://slack.pachyderm.io)

Official Python Pachyderm client. Created by [kalugny](https://github.com/kalugny) (formerly kalugny/pypachy.)

See the [API docs](https://pachyderm.github.io/python-pachyderm/python_pachyderm/). Most of values are auto-generated from protobufs. It's generally easier to rely on the higher-level classes if they provide the functionality you need:
* [PfsClient](https://pachyderm.github.io/python-pachyderm/python_pachyderm/pfs_client.m.html#python_pachyderm.pfs_client.PfsClient)
* [PpsClient](https://pachyderm.github.io/python-pachyderm/python_pachyderm/pps_client.m.html#python_pachyderm.pps_client.PpsClient)

## Installation

```bash
pip install python-pachyderm
```

## Usage and options

All of the PFS functions used in `pachctl` are supported (almost) as-is.

There are some helper functions that help make things more pythonic:

* `commit` which is a context manager wrapper for `start_commit` and `finish_commit`
* `get_files` which supports getting the data from multiple files

### Naming commits

All functions that accept a `commit` argument will accept a tuple of `(repo, branch)` or `(repo, commit_id)`,
a string like `repo/branch` or `repo/commit_id` and a Commit object.

e.g:

```python
client.list_file(('my_repo', 'branch'), '/')  # tuple
client.list_file('my_repo/commit_id', '/')    # string
c = client.list_commit('my_repo')[0]          # get some commit
client.list_file(c, '/')                      # and use it directly
```

### Basic usage example

```python
import python_pachyderm
client = python_pachyderm.PfsClient()
client.create_repo('test')
with client.commit('test', 'master') as c:
    client.put_file_bytes(c, '/dir_a/data', b'DATA')
    client.put_file_url(c, '/dir_b/icon.png', 'http://www.pearl-guide.com/forum/images/smilies/biggrin.png')

client.get_files('test/master', '/', recursive=True)
```

As of version 0.1.4, there is also limited support for PPS:

```python
pps_client = python_pachyderm.PpsClient()
pps_client.list_pipeline()
...
```

## Contributing

This driver is co-maintained by Pachyderm and the community. If you're looking to contribute to the project, this is a fantastic place to get involved.

### Getting started

To run tests, clone the repo, then run:

```
make init
tox
```
