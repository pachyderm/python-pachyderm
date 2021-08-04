# Spout Pipelines (Python)

This example showcases spouts and build pipelines in `python-pachyderm 6.x`. For more information on spouts, check out the Pachyderm docs and the example *[here](https://github.com/pachyderm/pachyderm/tree/master/examples/spouts/spout101)*.

**Prerequisites:**
- A workspace on [Pachyderm Hub](https://docs.pachyderm.com/latest/hub/hub_getting_started/) (recommended) or Pachyderm 1.x running [locally](https://docs.pachyderm.com/latest/getting_started/local_installation/)
- [python-pachyderm](https://pypi.org/project/python-pachyderm/) 6.x installed

**To run:**
```shell
$ python spout.py
```

Upon running `spout.py`, two pipelines are created, a producer (spout) pipeline and a consumer (regular) pipeline. The spout pipeline runs indefinitely.

The consumer pipeline is a special type of pipeline called a build pipeline, only available in `python-pachyderm`. Build pipelines automatically handle the steps of creating/pushing Docker images, baking in the source code and dependencies into the image.

### Producer pipeline
This pipeline appends a `#` to a file called `content` with each iteration of the while loop.

### Consumer pipeline
This pipeline writes the current datetime and the data from the `content` file to a file in the consumer output repo. This job is triggered each time the producer pipeline writes to the file.