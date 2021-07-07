import os
from pathlib import Path

from .proto.pps.pps_pb2 import Transform, CreatePipelineRequest, BuildSpec

from google.protobuf import json_format

# Default script for running python code with wheels in a pipeline that was
# deployed with `create_python_pipeline`.
RUNNER_SCRIPT_WITH_WHEELS = """
#!/bin/sh
set -{set_args}

cd /pfs/{source_repo_name}
pip install /pfs/{build_pipeline_name}/*.whl
python main.py
"""

# Default script for running python code without wheels in a pipeline that was
# deployed with `create_python_pipeline`.
RUNNER_SCRIPT_WITHOUT_WHEELS = """
#!/bin/sh
set -{set_args}

cd /pfs/{source_repo_name}
python main.py
"""

# Default script for building python wheels for a pipeline that was deployed
# with `create_python_pipeline`.
BUILDER_SCRIPT = """
#!/bin/sh
set -{set_args}
python --version
pip --version

cd /pfs/{source_repo_name}
test -f requirements.txt && pip wheel -r requirements.txt -w /pfs/out
"""


def put_files(client, source_path, commit, dest_path, **kwargs):
    """
    Utility function for inserting files from the local `source_path`
    to pachyderm. Roughly equivalent to `pachctl put file [-r]`.

    Params:

    * `client`: The `Client` instance to use.
    * `source_path`: The file/directory to recursively insert content from.
    * `commit`: The `Commit` object to use for inserting files.
    * `dest_path`: The destination path in PFS.
    * `kwargs`: Keyword arguments to forward. See
    `PutFileClient.put_file_from_fileobj` for details.
    """

    with client.put_file_client() as pfc:
        if os.path.isfile(source_path):
            pfc.put_file_from_filepath(commit, dest_path, source_path, **kwargs)
        elif os.path.isdir(source_path):
            for root, _, filenames in os.walk(source_path):
                for filename in filenames:
                    source_filepath = os.path.join(root, filename)
                    dest_filepath = os.path.join(
                        dest_path, os.path.relpath(source_filepath, start=source_path)
                    )
                    pfc.put_file_from_filepath(
                        commit, dest_filepath, source_filepath, **kwargs
                    )
        else:
            raise Exception("Please provide an existing directory or file")


def create_python_pipeline(
    client,
    path,
    input=None,
    pipeline_name=None,
    image_pull_secrets=None,
    debug=None,
    env=None,
    secrets=None,
    image=None,
    update=False,
    **pipeline_kwargs
):
    """
    Utility function for creating (or updating) a pipeline specially built for
    executing python code that is stored locally at `path`.

    A normal pipeline creation process requires you to first build and push a
    container image with the source and dependencies baked in. As an alternative
    process, this function circumvents container image creation by using build
    step-enabled pipelines. See the pachyderm core docs for more info.

    If `path` references a directory, it should have:

    * A `main.py`, as the pipeline entry-point.
    * An optional `requirements.txt` that specifies pip requirements.

    Params:

    * `client`: The `Client` instance to use.
    * `path`: The directory containing the python pipeline source, or an
    individual python file.
    * `input`: An optional `Input` object specifying the pipeline input.
    * `pipeline_name`: An optional string specifying the pipeline name.
    Defaults to using the last directory name in `path`.
    * `image_pull_secrets`: An optional list of strings specifying the
    pipeline transform's image pull secrets, which are used for pulling images
    from a private registry. Defaults to `None`, in which case the public
    docker registry will be used. See the pipeline spec document for more
    details.
    * `debug`: An optional bool specifying whether debug logging should be
    enabled for the pipeline. Defaults to `False`.
    * `env`: An optional mapping of string keys to string values specifying
    custom environment variables.
    * `secrets`: An optional list of `Secret` objects for secret environment
    variables.
    * `image`: An optional string specifying the docker image to use for the
    pipeline. Defaults to using pachyderm's official python language builder.
    * `update`: Whether to act as an upsert.
    * `pipeline_kwargs`: Keyword arguments to forward to `create_pipeline`.
    """

    return client.create_pipeline(
        pipeline_name or Path(path).name,
        Transform(
            image_pull_secrets=image_pull_secrets,
            debug=debug,
            env=env,
            secrets=secrets,
            build=BuildSpec(path=path, image=image)
            if image
            else BuildSpec(path=path, language="python"),
        ),
        update=update,
        input=input,
        **pipeline_kwargs
    )


def parse_json_pipeline_spec(j):
    """
    Parses a string of JSON into a `CreatePipelineRequest` protobuf.
    """
    return json_format.Parse(j, CreatePipelineRequest())


def parse_dict_pipeline_spec(d):
    """
    Parses a dict of serialized JSON into a `CreatePipelineRequest` protobuf.
    """
    return json_format.ParseDict(d, CreatePipelineRequest())
