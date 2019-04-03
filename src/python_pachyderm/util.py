import os

import six

from python_pachyderm.client.pfs import pfs_pb2 as pfs_proto


def get_address(host=None, port=None):
    if host is None or port is None:
        env_old_host = os.environ.get("PACHD_SERVICE_HOST")

        if env_old_host is not None:
            import warnings
            warnings.warn("The environment variable `PACHD_SERVICE_HOST` is deprecated; either explicitly set the host, or use `PACHD_ADDRESS`", DeprecationWarning)
            host = env_old_host

    if port is None:
        env_old_port = os.environ.get("PACHD_SERVICE_PORT_API_GRPC_PORT")

        if env_old_port is not None:
            import warnings
            warnings.warn("The environment variable `PACHD_SERVICE_PORT_API_GRPC_PORT` is deprecated; either explicitly set the host, or use `PACHD_ADDRESS`", DeprecationWarning)
            port = env_old_port

    if host is not None and port is not None:
        return "{}:{}".format(host, port)
    else:
        return os.environ.get("PACHD_ADDRESS", "localhost:30650")

def commit_from(src, allow_just_repo=False):
    if isinstance(src, pfs_proto.Commit):
        return src
    elif isinstance(src, (tuple, list)) and len(src) == 2:
        return pfs_proto.Commit(repo=pfs_proto.Repo(name=src[0]), id=src[1])
    elif isinstance(src, six.string_types):
        repo_name, commit_id = src.split('/', 1)
        return pfs_proto.Commit(repo=pfs_proto.Repo(name=repo_name), id=commit_id)
    if not allow_just_repo:
        raise ValueError(
            "Commit should either be a sequence of [repo, commit_id] or a string in the form 'repo/branch/commit_id")
    return pfs_proto.Commit(repo=pfs_proto.Repo(name=src))
