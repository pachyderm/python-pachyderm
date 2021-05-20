# Python Pachyderm

[![PyPI Package latest release](https://img.shields.io/pypi/v/python-pachyderm.svg)](https://pypi.python.org/pypi/python-pachyderm)
[![Supported versions](https://img.shields.io/pypi/pyversions/python-pachyderm.svg)](https://pypi.python.org/pypi/python-pachyderm)
[![Slack Status](https://badge.slack.pachyderm.io/badge.svg)](http://slack.pachyderm.io)

Official Python Pachyderm client. Created by [kalugny](https://github.com/kalugny) (formerly kalugny/pypachy), and now maintained by Pachyderm Inc.

This library provides the autogenerated gRPC/protobuf code for Pachyderm, along with a higher-level and more pythonic `Client` class.
See the [API docs](https://pachyderm.github.io/python-pachyderm/python_pachyderm.html).

## Installation

```bash
pip install python-pachyderm
```

## A Small Taste

Here's an example that creates a repo and adds a file:

```python
import python_pachyderm

# Connects to a pachyderm cluster on localhost:30650.
# For other options, see the API docs.
client = python_pachyderm.Client()

# Create a repo called `test`
client.create_repo('test')

# Upload a file to test/master where test is the repo and master is the branch
# /dir_a/data.txt is the path of the file at test/master
client.put_file_bytes("test/master", '/dir_a/data.txt', b'DATA')

# Get back the file
f = client.get_file("test/master", "/dir_a/data.txt")
print(f.read())  # >>> b"DATA"
```

How to load a CSV file into a Pandas dataframe

```Python
import pandas as pd

f = client.get_file("my_repo/my_branch", "/path_to/my_data.csv")
df = pd.read_csv(f)
```

For more sophisticated examples, [see the examples directory](https://github.com/pachyderm/python-pachyderm/tree/master/examples).

## Versioning

Prior to python-pachyderm 2.0, this library's versioning synced with pachyderm's core versioning; e.g. version 1.8.5 of this library synced with 1.8.5 of pachyderm core. python-pachyderm 2.0 onwards uses semver instead, so versions are not tied to pachyderm core. This was done for two reasons:

1. Sometimes this library makes breaking or backwards-incompatible changes, which aren't properly conveyed by revision changes.
2. Pachyderm core is stable enough that most features of this library will work for disparate versions of pachyderm clusters. To help ensure this, this library's CI tests against several versions of pachyderm core.

However, if for whatever reason you need to know which version of pachyderm core a version of python-pachyderm was built with, consult `CHANGELOG.md`. As a broad rule of thumb, we recommend working with the latest version of both pachyderm core and python-pachyderm where possible.

## Contributing

This driver is co-maintained by Pachyderm and the community. If you're looking to contribute to the project, this is a fantastic place to get involved. Take a look at [the contributing guide](./contributing.md) for more info (including testing instructions).
