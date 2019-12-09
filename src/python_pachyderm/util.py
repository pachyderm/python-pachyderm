import io
import os
import sys
import json
import uuid
import tarfile
import tempfile
import collections

from .client import Client
from .proto.pps.pps_pb2 import Input, Transform, PFSInput

# Script for running python code in a pipeline that was deployed with
# `build_pipeline`.
RUNNER_SCRIPT = """
#!/bin/bash
set -e{}
cd /pfs/{}
test -d wheelhouse && pip3 install wheelhouse/*.whl
test -f requirements.txt && pip3 install -r requirements.txt
python3 main.py
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
    Utility function for creating (or updating) a pipeline for executing python
    code. Instead of baking a container image with the source code, this
    inserts source code into a PFS repo, then uses that repo to run the
    pipeline. If you want to easily push out python code stored locally on
    your computer, this is oftentimes more convenient than using
    `client.create_pipeline`, since you don't have to deal with building and
    pushing container images; however, note that pipelines built with this
    will tend to run slower, as they have to pip install their dependencies on
    every run (versus images which tend to have their dependencies pre-baked.)

    The directory at the specified `path` should have the following:

    * A `main.py`, as the pipeline entry-point.
    * An optional `requirements.txt` that specifies pip requirements.
    * An optional `wheelhouse` directory that contains wheels (`.whl` files)
    to be installed as well, which can be used to help speed dependency
    resolution up. Note that wheels in here must target the same platform as
    the pipeline execution environment (which defaults to the docker image
    `python`, unless overridden via the `image` argument.)

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

    if not os.path.exists(path):
        raise Exception("path does not exist")

    if pipeline_name is None:
        if not path.endswith("/"):
            pipeline_name = os.path.basename(path)
        else:
            pipeline_name = os.path.basename(path[:-1])

    if not pipeline_name:
        raise Exception("could not derive pipeline name")

    source_repo = "{}_source".format(pipeline_name)
    commit = None
    create_source_repo = True

    if update:
        try:
            client.inspect_repo(source_repo)
            create_source_repo = False
        except Exception as e:
            if "not found" not in e:
                raise

        if not create_source_repo:
            commit = client.start_commit(source_repo, branch="master", description="python_pachyderm: sync source code")

            for file_info in client.walk_file(commit, "/"):
                client.delete_file(commit, file_info.file.path)

    if create_source_repo:
        client.create_repo(source_repo, description="python_pachyderm: source code for pipeline {}".format(pipeline_name))

    if commit is None:
        commit = client.start_commit(source_repo, branch="master", description="python_pachyderm: add source code")

    put_files(client, path, commit, "/")

    if not os.path.exists(os.path.join(path, "run.sh")):
        with io.BytesIO(RUNNER_SCRIPT.format("x" if debug else "", source_repo).encode("utf8")) as runner_f:
            client.put_file_bytes(commit, "/run.sh", runner_f)

    client.finish_commit(commit)

    transform = Transform(
        image=image or "python",
        cmd=["bash", "/pfs/{}/run.sh".format(source_repo)],
        image_pull_secrets=image_pull_secrets,
        debug=debug,
    )

    input = Input(cross=[
        Input(pfs=PFSInput(glob="/", repo=source_repo)),
        input,
    ])

    return client.create_pipeline(
        pipeline_name,
        transform,
        input=input,
        update=update,
        **(pipeline_kwargs or {})
    )
