import io
import os
import sys
import json
import uuid
import tarfile
import tempfile
import collections

from .client import Client
from .proto.pps.pps_pb2 import Input, Transform, PFSInput, ParallelismSpec

# Script for running python code in a pipeline that was deployed with
# `build_pipeline`.
RUNNER_SCRIPT = """
#!/bin/bash
set -e{}

cd /pfs/{}
pip install wheelhouse/*.whl
test -f requirements.txt && pip install -r requirements.txt
python main.py
"""

BUILDER_SCRIPT = """
#!/bin/bash
set -e{}
python --version
pip --version

mv /pfs/{}/* /pfs/out/
cd /pfs/out
test -d wheelhouse || mkdir -p wheelhouse
test -f requirements.txt && pip wheel -r requirements.txt -w wheelhouse
"""

def put_files(client, source_path, commit, dest_path, **kwargs):
    """
    Utility function for recursively inserting files from the local
    `source_path` to pachyderm. Roughly equivalent to `pachctl put file -r`.

    Params:

    * `client`: The `Client` instance to use.
    * `source_path`: The directory to recursively insert content from.
    * `commit`: The `Commit` object to use for inserting files.
    * `dest_path`: The destination path in PFS.
    * `kwargs`: Keyword arguments to forward to `put_file_bytes`.
    """

    for root, _, filenames in os.walk(source_path):
        for filename in filenames:
            source_filepath = os.path.join(root, filename)
            dest_filepath = os.path.join(dest_path, os.path.relpath(source_filepath, start=source_path))

            with open(source_filepath, "rb") as f:
                client.put_file_bytes(commit, dest_filepath, f, **kwargs)

def build_python_pipeline(client, path, input, pipeline_name=None, image_pull_secrets=None, debug=None, pipeline_kwargs=None, image=None, update=False):
    """
    Utility function for creating (or updating) a pipeline specially built for
    executing python code that is stored locally.

    Instead of baking a container with the source code and dependencies, this
    creates:

    1) a repo for storing the source code.
    2) A pipeline for building the dependencies into wheels.
    3) A pipeline for executing the PFS stored source code with the built
    dependencies.

    As a result, this is what the pachyderm DAG looks like:

    ```
    +------------------------+      +-----------------------+
    |                        |      |                       |
    | <pipeline_name>_source | ---> | <pipeline_name>_build |
    |                        |      |                       | \
    +------------------------+      +-----------------------+  \       +-----------------+
                                                                \      |                 |
                                                                  ---> | <pipeline_name> |
                                                                /      |                 |
                                                  +---------+  /       +-----------------+
                                                  |         | /
                                                  | <input> |
                                                  |         |
                                                  +---------+

    ```

    This setup is oftentimes more convenient than using
    `client.create_pipeline`, since you don't have to deal with building and
    pushing container images; however, note the caveats:

    * Pipeline execution will be slower for dependencies that cannot be
    resolved as wheels, since they need to be re-pulled on every pipeline run.
    * This creates an extra repo and an extra pipeline for each pipeline.

    The directory at the specified `path` should have the following:

    * A `main.py`, as the pipeline entry-point.
    * An optional `requirements.txt` that specifies pip requirements.
    * An optional `wheelhouse` directory that contains wheels (`.whl` files)
    to be installed as well, which can be used to help speed the build
    process (since those wheels don't need to be resolved.) Note that wheels
    in here must target the same platform as the pipeline execution
    environment (which defaults to the docker image `python`, unless
    overridden via the `image` argument.)

    If you need further customization, you can override behavior by specifying
    one or both of these scripts in `path`:

    * `build.sh`, which is run by the build pipeline to build wheels
    * `run.sh`, which is run by the pipeline you're creating to execute the
    python code.

    Params:

    * `client`: The `Client` instance to use.
    * `path`: The directory containing the python pipeline source.
    * `input`: An `Input` object specifying the pipeline input.
    * `pipeline_name`: An optional string specifying the pipeline name.
    Defaults to using the last directory name in `path`.
    * `image_pull_secrets`: An optional list of strings specifying the
    pipeline transform's image pull secrets, which are used for pulling images
    from a private registry. Defaults to `None`, in which case the public
    docker registry will be used. See the pipeline spec document for more
    details.
    * `debug`: An optional bool specifying whether debug logging should be
    enabled for the pipeline. Defaults to `False`.
    * `pipeline_kwargs`: Keyword arguments to forward to `create_pipeline`.
    * `image`: An optional string specifying the docker image to use for the
    pipeline. Defaults to `python`.
    * `update`: Whether to act as an upsert.
    """

    # Verify & set defaults for arguments
    if not os.path.exists(path):
        raise Exception("path does not exist")

    if pipeline_name is None:
        if not path.endswith("/"):
            pipeline_name = os.path.basename(path)
        else:
            pipeline_name = os.path.basename(path[:-1])

    if not pipeline_name:
        raise Exception("could not derive pipeline name")

    image = image or "python:3"
    pipeline_kwargs = pipeline_kwargs or {}

    # Create the source repo and build pipeline (if necessary.)
    source_repo_name = "{}_source".format(pipeline_name)
    build_pipeline_name = "{}_build".format(pipeline_name)
    create_source_repo = True
    create_build_pipeline = True
    commit = None

    if update:
        try:
            client.inspect_repo(source_repo_name)
            create_source_repo = False
        except Exception as e:
            if "not found" not in e:
                raise

        try:
            client.inspect_pipeline(build_pipeline_name)
            create_build_pipeline = False
        except Exception as e:
            if "not found" not in e:
                raise

        if not create_source_repo:
            # The source repo already exists - delete existing source code
            # (since upsert is enabled.)
            commit = client.start_commit(
                source_repo_name,
                branch="master",
                description="python_pachyderm.build_python_pipeline: sync source code.",
            )
            client.delete_file(commit, "/")

    if create_source_repo:
        client.create_repo(
            source_repo_name,
            description="python_pachyderm.build_python_pipeline: source code for pipeline {}.".format(pipeline_name),
        )

    if create_build_pipeline:
        client.create_pipeline(
            build_pipeline_name,
            Transform(
                image=image,
                cmd=["bash", "/pfs/{}/build.sh".format(source_repo_name)],
                image_pull_secrets=image_pull_secrets,
                debug=debug,
            ),
            input=Input(pfs=PFSInput(glob="/", repo=source_repo_name)),
            update=update,
            description="python_pachyderm.build_python_pipeline: build artifacts for pipeline {}.".format(pipeline_name),
            parallelism_spec=ParallelismSpec(constant=1),
        )

    # If we haven't created a commit already, create one now to insert the
    # source code
    if commit is None:
        commit = client.start_commit(
            source_repo_name,
            branch="master",
            description="python_pachyderm.build_python_pipeline: add source code.",
        )

    # Insert the source code
    put_files(client, path, commit, "/")

    # Insert either the user-specified `build.sh`, or the default one
    if not os.path.exists(os.path.join(path, "build.sh")):
        with io.BytesIO(BUILDER_SCRIPT.format("x" if debug else "", source_repo_name).encode("utf8")) as builder_file:
            client.put_file_bytes(commit, "/build.sh", builder_file)

    # Insert either the user-specified `run.sh`, or the default one
    if not os.path.exists(os.path.join(path, "run.sh")):
        with io.BytesIO(RUNNER_SCRIPT.format("x" if debug else "", build_pipeline_name).encode("utf8")) as runner_file:
            client.put_file_bytes(commit, "/run.sh", runner_file)

    client.finish_commit(commit)

    # Create the pipeline
    return client.create_pipeline(
        pipeline_name,
        Transform(
            image=image,
            cmd=["bash", "/pfs/{}/run.sh".format(build_pipeline_name)],
            image_pull_secrets=image_pull_secrets,
            debug=debug,
        ),
        input=Input(cross=[Input(pfs=PFSInput(glob="/", repo=build_pipeline_name)), input]),
        update=update,
        **pipeline_kwargs
    )
