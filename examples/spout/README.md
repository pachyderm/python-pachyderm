>![pach_logo](../img/pach_logo.svg) INFO: python-pachyderm 7.x introduces profound architectual changes to the product. As a result, our examples are kept in two separate branches:
> - Branch Master: Examples using python-pachyderm 7.x - https://github.com/pachyderm/python-pachyderm/tree/master/examples
> - Branch v6.x: Examples using python-pachyderm 6.x - https://github.com/pachyderm/python-pachyderm/tree/v6.x/examples

# Spout Pipelines (Python)

This example is a reproduction of the Spouts101 example from the Pachyderm repo. This example uses the python-pachyderm analogs for creating pipelines (`spout.py`), which was done using `pachctl` commands in the Spouts101 example. For more information on Spouts or for a full walkthrough of the original example, go *[here](https://github.com/pachyderm/pachyderm/tree/master/examples/spouts/spout101)*.

**Prerequisites:**
- A workspace on [Pachyderm Hub](https://docs.pachyderm.com/latest/hub/hub_getting_started/) (recommended) or Pachyderm running [locally](https://docs.pachyderm.com/latest/getting_started/local_installation/)
- [python-pachyderm](https://pypi.org/project/python-pachyderm/) installed

**To run:**
```shell
$ python spout.py
```