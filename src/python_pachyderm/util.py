import os

from google.protobuf import json_format

from python_pachyderm import Client
from python_pachyderm.proto.v2.pps.pps_pb2 import CreatePipelineRequest


def put_files(client: Client, source_path, commit, dest_path, **kwargs):
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

    with client.modify_file_client(commit) as pfc:
        if os.path.isfile(source_path):
            pfc.put_file_from_filepath(dest_path, source_path, **kwargs)
        elif os.path.isdir(source_path):
            for root, _, filenames in os.walk(source_path):
                for filename in filenames:
                    source_filepath = os.path.join(root, filename)
                    dest_filepath = os.path.join(
                        dest_path, os.path.relpath(source_filepath, start=source_path)
                    )
                    pfc.put_file_from_filepath(dest_filepath, source_filepath, **kwargs)
        else:
            raise Exception("Please provide an existing directory or file")


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
