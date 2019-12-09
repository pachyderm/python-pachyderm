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

RUNNER_SCRIPT = """
#!/bin/bash
set -e{}
cd /pfs/{}
test -f requirements.txt && pip3 install -r requirements.txt
python3 main.py
"""

def put_files(client, source_path, commit, dest_path, **kwargs):
    for root, _, filenames in os.walk(source_path):
        for filename in filenames:
            source_filepath = os.path.join(root, filename)
            dest_filepath = os.path.join(dest_path, os.path.relpath(source_filepath, start=source_path))

            with open(source_filepath, "rb") as f:
                client.put_file_bytes(commit, dest_filepath, f, **kwargs)

def build_pipeline(client, path, input, pipeline_name=None, image_pull_secrets=None, debug=None, pipeline_kwargs=None, image=None, update=False):
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
