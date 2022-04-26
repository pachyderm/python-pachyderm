import os
from shutil import which
from subprocess import run
from typing import Union, TYPE_CHECKING

from python_pachyderm.experimental.service import pps_proto, pfs_proto

if TYPE_CHECKING:
    # Avoid circular import
    from python_pachyderm.experimental import Client
    from python_pachyderm.experimental.pfs import Commit


def put_files(
    client: "Client",
    source_path: str,
    commit: Union[tuple, dict, "Commit", pfs_proto.Commit],
    dest_path: str,
    **kwargs
) -> None:
    """Utility function for inserting files from the local `source_path`
    into Pachyderm. Roughly equivalent to ``pachctl put file [-r]``.

    Parameters
    ----------
    client : Client
        A python_pachyderm client instance.
    source_path : str
        The file/directory to recursively insert content from.
    commit : Union[tuple, dict, Commit, pfs_proto.Commit]
        The open commit to add files to.
    dest_path : str
        The destination path in PFS.
    **kwargs : dict
        Keyword arguments to forward. See
        ``ModifyFileClient.put_file_from_filepath()`` for more details.

    Examples
    --------
    >>> source_dir = "data/training/"
    >>> with client.commit("repo_name", "master") as commit:
    >>>     python_pachyderm.put_files(client, source_dir, commit, "/training_set/")
    ...
    >>> with client.commit("repo_name", "master") as commit2:
    >>>     python_pachyderm.put_files(client, "metadata/params.csv", commit2, "/hyperparams.csv")
    >>>     python_pachyderm.put_files(client, "spec.json", commit2, "/")

    .. # noqa: W505
    """
    with client.modify_file_client(commit) as mfc:
        if os.path.isfile(source_path):
            mfc.put_file_from_filepath(dest_path, source_path, **kwargs)
        elif os.path.isdir(source_path):
            for root, _, filenames in os.walk(source_path):
                for filename in filenames:
                    source_filepath = os.path.join(root, filename)
                    dest_filepath = os.path.join(
                        dest_path, os.path.relpath(source_filepath, start=source_path)
                    )
                    mfc.put_file_from_filepath(dest_filepath, source_filepath, **kwargs)
        else:
            raise Exception("Please provide an existing directory or file")


def parse_json_pipeline_spec(j: str) -> pps_proto.CreatePipelineRequest:
    """Parses a string of JSON into a `CreatePipelineRequest` protobuf.

    Parameters
    ----------
    j : str
        Pipeline spec as a JSON-like string.


    Returns
    -------
    pps_proto.CreatePipelineRequest
        A protobuf object that contains the spec info necessary to create a
        pipeline.

    Examples
    --------
    Useful for going from Pachyderm spec to creating a pipeline. Pachyderm
    spec: https://docs.pachyderm.com/latest/reference/pipeline_spec/

    >>> spec = '''{
    ...     "pipeline": {
    ...         "name": "foobar"
    ...     },
    ...     "description": "A pipeline that performs image edge detection by using the OpenCV library.",
    ...     "input": {
    ...         "pfs": {
    ...         "glob": "/*",
    ...         "repo": "images"
    ...         }
    ...     },
    ...     "transform": {
    ...         "cmd": [ "python3", "/edges.py" ],
    ...         "image": "pachyderm/opencv"
    ...     }
    ... }'''
    >>> req = python_pachyderm.parse_json_pipeline_spec(spec)
    >>> client.create_pipeline_from_request(req)

    .. # noqa: W505
    """
    return pps_proto.CreatePipelineRequest().from_json(j)


def parse_dict_pipeline_spec(d: dict) -> pps_proto.CreatePipelineRequest:
    """Parses a dict of serialized JSON into a `CreatePipelineRequest` protobuf.

    Parameters
    ----------
    d : dict
        Pipeline spec as a dictionary.

    Returns
    -------
    pps_proto.CreatePipelineRequest
        A protobuf object that contains the spec info necessary to create a
        pipeline.

    Examples
    --------
    Useful for going from Pachyderm spec to creating a pipeline. Pachyderm
    spec: https://docs.pachyderm.com/latest/reference/pipeline_spec/

    >>> spec = '''{
    ...     "pipeline": {
    ...         "name": "foobar"
    ...     },
    ...     "description": "A pipeline that performs image edge detection by using the OpenCV library.",
    ...     "input": {
    ...         "pfs": {
    ...         "glob": "/*",
    ...         "repo": "images"
    ...         }
    ...     },
    ...     "transform": {
    ...         "cmd": [ "python3", "/edges.py" ],
    ...         "image": "pachyderm/opencv"
    ...     }
    ... }'''
    >>> req = python_pachyderm.parse_dict_pipeline_spec(json.loads(spec))
    >>> client.create_pipeline_from_request(req)

    .. # noqa: W505
    """

    return pps_proto.CreatePipelineRequest().from_dict(d)


def check_pachctl() -> None:
    """Ensures that pachctl is installed and is capable of running
    mount operations.

    Errors
    ------
    FileNotFoundError
        No pachctl binary present
    RuntimeError
        pachctl binary present but cannot run mount operations
    """
    if which("pachctl") is None:
        raise FileNotFoundError("pachctl")
    if run(["pachctl", "mount", "-h"], capture_output=True).returncode != 0:
        raise RuntimeError("incompatible pachctl detected")
