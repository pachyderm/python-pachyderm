import io
import os

from .proto.pps.pps_pb2 import Input, Transform, PFSInput, ParallelismSpec

# Default script for running python code with wheels in a pipeline that was
# deployed with `create_python_pipeline`.
RUNNER_SCRIPT_WITH_WHEELS = """
#!/bin/bash
set -{set_args}

cd /pfs/{source_repo_name}
pip install /pfs/{build_pipeline_name}/*.whl
python main.py
"""

# Default script for running python code without wheels in a pipeline that was
# deployed with `create_python_pipeline`.
RUNNER_SCRIPT_WITHOUT_WHEELS = """
#!/bin/bash
set -{set_args}

cd /pfs/{source_repo_name}
python main.py
"""

# Default script for building python wheels for a pipeline that was deployed
# with `create_python_pipeline`.
BUILDER_SCRIPT = """
#!/bin/bash
set -{set_args}
python --version
pip --version

cd /pfs/{source_repo_name}
test -f requirements.txt && pip wheel -r requirements.txt -w /pfs/out
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


def create_python_pipeline(client, path, input, pipeline_name=None, image_pull_secrets=None, debug=None,
                           pipeline_kwargs=None, image=None, update=False):
    """
    Utility function for creating (or updating) a pipeline specially built for
    executing python code that is stored locally at `path`. `path` can either
    reference a directory with python code, or a single python file.

    A normal pipeline creation process (i.e. a call to
    `client.create_pipeline`) requires you to first build and push a container
    image with the source and dependencies baked in. As an alternative
    process, this function circumvents container image creation by creating:

    1) a PFS repo that stores the source code at `path`.
    2) If there's a `requirements.txt` in `path`, a  pipeline for building the
    dependencies into wheels.
    3) A pipeline for executing the PFS stored source code with the built
    dependencies.

    This is what the DAG looks like:

    ```
    .------------------------.      .-----------------------.
    | <pipeline_name>_source | ---▶ | <pipeline_name>_build |
    '------------------------'      '-----------------------'
                 |                 /
                 ▼                /
        .-----------------.      /
        | <pipeline_name> | ◀---'
        '-----------------'
                 ▲
                 |
            .---------.
            | <input> |
            '---------'

    ```

    (without a `requirements.txt`, there is no build pipeline.)

    If `path` references a directory, it should have following:

    * A `main.py`, as the pipeline entry-point.
    * An optional `requirements.txt` that specifies pip requirements.
    * An optional `build.sh` if you wish to override the default build
    process.
    * An optional `run.sh` if you wish to override the default pipeline
    execution process.

    Params:

    * `client`: The `Client` instance to use.
    * `path`: The directory containing the python pipeline source, or an
    individual python file.
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
        if path.endswith("/"):
            pipeline_name = os.path.basename(path[:-1])
        elif path.endswith(".py"):
            pipeline_name = os.path.basename(path)[:-3]
        else:
            pipeline_name = os.path.basename(path)

    if not pipeline_name:
        raise Exception("could not derive pipeline name")

    image = image or "python:3"
    pipeline_kwargs = pipeline_kwargs or {}

    # Create the source repo and build pipeline (if necessary.)
    source_repo_name = "{}_source".format(pipeline_name)
    build_pipeline_name = "{}_build".format(pipeline_name) if os.path.exists(os.path.join(path, "requirements.txt")) else None

    client.create_repo(
        source_repo_name,
        description="python_pachyderm.create_python_pipeline: source code for pipeline {}.".format(pipeline_name),
        update=update,
    )

    if build_pipeline_name is not None:
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
            description="""
                python_pachyderm.create_python_pipeline: build artifacts for pipeline {}.
            """.format(pipeline_name).strip(),
            parallelism_spec=ParallelismSpec(constant=1),
        )

    with client.commit(source_repo_name, branch="master", description="python_pachyderm.create_python_pipeline: sync source code.") as commit:
        # Utility function for inserting build.sh/run.sh
        def put_templated_script(filename, template):
            source = template.format(
                set_args="ex" if debug else "e",
                source_repo_name=source_repo_name,
                build_pipeline_name=build_pipeline_name,
            )

            with io.BytesIO(source.encode("utf8")) as f:
                client.put_file_bytes(commit, filename, f)

        # Delete any existing source code
        if update:
            client.delete_file(commit, "/")

        # Insert the source code
        if build_pipeline_name is None:
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    client.put_file_bytes(commit, "main.py", f)
            else:
                put_files(client, path, commit, "/")

            put_templated_script("run.sh", RUNNER_SCRIPT_WITHOUT_WHEELS)
        else:
            put_files(client, path, commit, "/")

            if not os.path.exists(os.path.join(path, "run.sh")):
                put_templated_script("run.sh", RUNNER_SCRIPT_WITH_WHEELS)
            if not os.path.exists(os.path.join(path, "build.sh")):
                put_templated_script("build.sh", BUILDER_SCRIPT)

    # Create the pipeline
    inputs = [Input(pfs=PFSInput(glob="/", repo=source_repo_name)), input]

    if build_pipeline_name is not None:
        inputs.append(Input(pfs=PFSInput(glob="/", repo=build_pipeline_name)))

    return client.create_pipeline(
        pipeline_name,
        Transform(
            image=image,
            cmd=["bash", "/pfs/{}/run.sh".format(source_repo_name)],
            image_pull_secrets=image_pull_secrets,
            debug=debug,
        ),
        input=Input(cross=inputs),
        update=update,
        **pipeline_kwargs
    )
