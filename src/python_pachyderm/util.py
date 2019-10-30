from enum import Enum

from python_pachyderm.proto.admin import admin_pb2 as admin_proto
from python_pachyderm.proto.admin import admin_pb2_grpc as admin_grpc
from python_pachyderm.proto.pfs import pfs_pb2 as pfs_proto
from python_pachyderm.proto.pfs import pfs_pb2_grpc as pfs_grpc
from python_pachyderm.proto.pps import pps_pb2 as pps_proto
from python_pachyderm.proto.pps import pps_pb2_grpc as pps_grpc
from python_pachyderm.proto.transaction import transaction_pb2 as transaction_proto
from python_pachyderm.proto.transaction import transaction_pb2_grpc as transaction_grpc
from python_pachyderm.proto.version.versionpb import version_pb2_grpc as version_grpc


class Service(Enum):
    ADMIN = 0
    PFS = 1
    PPS = 2
    TRANSACTION = 3
    VERSION = 4


GRPC_MODULES = {
    Service.ADMIN: admin_grpc,
    Service.PFS: pfs_grpc,
    Service.PPS: pps_grpc,
    Service.TRANSACTION: transaction_grpc,
    Service.VERSION: version_grpc,
}

PROTO_MODULES = {
    Service.ADMIN: admin_proto,
    Service.PFS: pfs_proto,
    Service.PPS: pps_proto,
    Service.TRANSACTION: transaction_proto,
}


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
