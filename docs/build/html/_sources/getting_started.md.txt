# Getting Started

## Prerequisites
* A workspace on [Pachyderm Hub](https://docs.pachyderm.com/latest/hub/hub_getting_started/) (recommended) or Pachyderm running [locally](https://docs.pachyderm.com/latest/getting_started/local_installation/)
* Python 3.6 or higher

## Installation
* via PyPI: `pip install python-pachyderm`
* via source
    * download desired version [here](https://github.com/pachyderm/python-pachyderm/releases) (under assets)
    * `make init` in folder root-level

## Hello World example

Creates a repo, commits data to the repo (versioning the data), and reads the data back from the repo.

```python
import python_pachyderm

# Connects to a pachyderm cluster on localhost:30650
# For other connection options, see the API docs
client = python_pachyderm.Client()

# Create a pachyderm repo called `test`
client.create_repo("test")

# Create a file in (repo="test", branch="master") at `/dir_a/data.txt`
# Similar to `pachctl put file test@master:/dir_a/data.txt`
with client.commit("test", "master") as commit:
    client.put_file_bytes(commit, "/dir_a/data.txt", b"hello world")

# Get the file
f = client.get_file(("test", "master"), "/dir_a/data.txt")
print(f.read())  # >>> b"hello world"
```

Since `client.get_file()` returns a file-like object, you can pass it into your favorite analysis packages.

```python
import pandas as pd

f = client.get_file(("my_repo", "my_branch"), "/path_to/my_data.csv")
df = pd.read_csv(f)
```

## Hello World example with Pachyderm Pipelines

Creates a data-driven pipeline that transforms the data and outputs results to a new repo. This example pipeline counts the occurrences of the word `hello` in the repo. Continued from the example above...

```python
from python_pachyderm.service import pps_proto

# Create a pipeline that logs frequency of the word "hello" in `test`
# repo to a file in the `word_count` repo (which is created automatically)
# Any time data is committed to the `test` repo, this pipeline will
# automatically trigger.
client.create_pipeline(
    "word_count",
    transform=pps_proto.Transform(
        cmd=["bash"],
        stdin=[
            "grep -roh hello /pfs/test/ | wc -w > /pfs/out/count.txt"
        ]
    ),
    input=pps_proto.Input(
        pfs=pps_proto.PFSInput(repo="test", branch="master", glob="/")
    )
)

# Wait for new commit, triggered by pipeline run, to finish
client.wait_commit(("word_count", "master"))

# Check `count.txt` for "hello" count
f = client.get_file(("word_count", "master"), "count.txt")
print(f.read())  # >>> b"1"

# Add more data to the `test` repo
with client.commit("test", "master") as commit:
    client.put_file_bytes(commit, "/data2.txt", b"hello hello from the top of the world")

# Wait for commit to finish
client.wait_commit(commit.id)

# Check `count.txt` for "hello" count
f = client.get_file(("word_count", "master"), "count.txt")
print(f.read())  # >>> b"3"
```

For more sophisticated examples, [see the examples directory](https://github.com/pachyderm/python-pachyderm/tree/master/examples).
To learn more about what you can do with Pachyderm, check out the [docs](https://docs.pachyderm.com/latest/how-tos/).