import os

from python_pachyderm._proto.pfs import pfs_pb2 as pfs_proto


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
    elif isinstance(src, str):
        repo_name, commit_id = src.split('/', 1)
        return pfs_proto.Commit(repo=pfs_proto.Repo(name=repo_name), id=commit_id)

    if not allow_just_repo:
        raise ValueError("Invalid commit type")
    return pfs_proto.Commit(repo=pfs_proto.Repo(name=src))
