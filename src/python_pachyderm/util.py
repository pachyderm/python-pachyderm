import os

import six

from python_pachyderm.client.pfs import pfs_pb2 as pfs_proto
from python_pachyderm.client.version.versionpb.version_pb2_grpc import (
    google_dot_protobuf_dot_empty__pb2 as pb_empty,
    APIStub as VersionStub,
    grpc
)
from contextlib import closing


def get_address(host=None, port=None):
    if host is not None and port is not None:
        return "{}:{}".format(host, port)
    return os.environ.get("PACHD_ADDRESS", "localhost:30650")


def get_metadata(auth_token=None):
    if auth_token is None:
        auth_token = os.environ.get("PACH_PYTHON_AUTH_TOKEN")

    metadata = []
    if auth_token is not None:
        metadata.append(("authn-token", auth_token))

    return metadata


def commit_from(src, allow_just_repo=False):
    if isinstance(src, pfs_proto.Commit):
        return src
    elif isinstance(src, (tuple, list)) and len(src) == 2:
        return pfs_proto.Commit(repo=pfs_proto.Repo(name=src[0]), id=src[1])
    elif isinstance(src, six.string_types):
        repo_name, commit_id = src.split('/', 1)
        return pfs_proto.Commit(repo=pfs_proto.Repo(name=repo_name), id=commit_id)

    if not allow_just_repo:
        raise ValueError("Invalid commit type")
    return pfs_proto.Commit(repo=pfs_proto.Repo(name=src))


def get_remote_version(host=None, port=None):
    with closing(grpc.insecure_channel(get_address(host, port))) as channel:
        stub = VersionStub(channel)
        version = stub.GetVersion(pb_empty.Empty())
        return version
