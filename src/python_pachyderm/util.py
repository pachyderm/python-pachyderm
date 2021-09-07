import os
from typing import Union

from google.protobuf import json_format

from python_pachyderm import Client
from python_pachyderm.pfs import Commit
from python_pachyderm.service import pps_proto, pfs_proto


def put_files(
    client: Client,
    source_path: str,
    commit: Union[tuple, dict, Commit, pfs_proto.Commit],
    dest_path: str,
    **kwargs
) -> None:
    """Utility function for inserting files from the local `source_path`
    to Pachyderm. Roughly equivalent to ``pachctl put file [-r]``.

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
        ``ModifyFileClient.put_file_from_filepath`` for more details.
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
    """
    return json_format.Parse(j, pps_proto.CreatePipelineRequest())


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
    """
    return json_format.ParseDict(d, pps_proto.CreatePipelineRequest())
