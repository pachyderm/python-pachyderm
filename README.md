# PyPachy

A python client wrapper for [Pachyderm](https://www.pachyderm.io/) API.

Currently implements only the PFS interface.

## Installing

```bash
$ git clone https://github.com/kalugny/pypachy.git
$ cd pypachy
$ python setup.py install
```

## Instructions
The functions correspond closely to the Go client implementation and are very similar to the
`pachctl` interface as well.

In any place where a `commit` is expected you can either put a sequence in the form of `(repo_name, branch/commit_id)` or 
a string in the form of `repo/branch/commit_id`. 

Usage example:

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
>>> client.put_file('test/master/0', 'test/text.txt', pypachy.FILE_TYPE_REGULAR, value=b'Hello')
>>> client.finish_commit('test/master/0')
>>> list(client.get_file('test/master/0', 'test/text.txt' ))
[value: "Hello"]
```

## TODO
* Make the output more Pythonic - especially for "stream" types like `get_file`
* Add support for `BlockAPI`
* Add support for `PPS`, `Version`, etc