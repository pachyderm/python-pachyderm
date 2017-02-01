# PyPachy

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

>>> client.put_file('test/master/0', 'test', pypachy.FILE_TYPE_DIR)
>>> client.put_file('test3/master/0', 'test/text.txt', pypachy.FILE_TYPE_REGULAR, value=b'Hello')
>>> client.finish_commit('test/master/0')
>>> list(client.get_file('test', 'master/0', 'test/text.txt' ))
[value: "Hello"]
```

## TODO
* Make the output more Pythonic - especially for "stream" types like `get_file`
* Add support for `BlockAPI`
* Add support for `PPS`, `Version`, etc