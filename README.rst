PyPachy
=======

A python client wrapper for [Pachyderm](https://www.pachyderm.io/) API.

Currently implements only the PFS interface.

Usage:

```python

>>> import pypachy

>>> client = pypachy.PfsClient()
>>> client.create_repo('test')
>>> client.start_commit('test', 'master')
repo {
  name: "test"
}
id: "master/0"

```