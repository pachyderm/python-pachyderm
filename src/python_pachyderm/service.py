from enum import Enum

from grpc_health.v1 import health_pb2 as health_proto
from grpc_health.v1 import health_pb2_grpc as health_grpc
from python_pachyderm.proto.v2.admin import admin_pb2 as admin_proto
from python_pachyderm.proto.v2.admin import admin_pb2_grpc as admin_grpc
from python_pachyderm.proto.v2.auth import auth_pb2 as auth_proto
from python_pachyderm.proto.v2.auth import auth_pb2_grpc as auth_grpc
from python_pachyderm.proto.v2.debug import debug_pb2 as debug_proto
from python_pachyderm.proto.v2.debug import debug_pb2_grpc as debug_grpc
from python_pachyderm.proto.v2.enterprise import enterprise_pb2 as enterprise_proto
from python_pachyderm.proto.v2.enterprise import enterprise_pb2_grpc as enterprise_grpc
from python_pachyderm.proto.v2.identity import identity_pb2 as identity_proto
from python_pachyderm.proto.v2.identity import identity_pb2_grpc as identity_grpc
from python_pachyderm.proto.v2.license import license_pb2 as license_proto
from python_pachyderm.proto.v2.license import license_pb2_grpc as license_grpc
from python_pachyderm.proto.v2.pfs import pfs_pb2 as pfs_proto
from python_pachyderm.proto.v2.pfs import pfs_pb2_grpc as pfs_grpc
from python_pachyderm.proto.v2.pps import pps_pb2 as pps_proto
from python_pachyderm.proto.v2.pps import pps_pb2_grpc as pps_grpc
from python_pachyderm.proto.v2.transaction import transaction_pb2 as transaction_proto
from python_pachyderm.proto.v2.transaction import (
    transaction_pb2_grpc as transaction_grpc,
)
from python_pachyderm.proto.v2.version.versionpb import version_pb2 as version_proto
from python_pachyderm.proto.v2.version.versionpb import version_pb2_grpc as version_grpc

MB = 1024 ** 2
MAX_RECEIVE_MESSAGE_SIZE = 20 * MB


class Service(Enum):
    ADMIN = 0
    AUTH = 1
    DEBUG = 2
    HEALTH = 3
    ENTERPRISE = 4
    PFS = 5
    PPS = 6
    TRANSACTION = 7
    VERSION = 8
    LICENSE = 9
    IDENTITY = 10

    @property
    def grpc_module(self):
        return GRPC_MODULES[self]

    @property
    def stub(self):
        grpc_module = self.grpc_module

        for key in dir(grpc_module):
            if key.endswith("Stub"):
                return getattr(self.grpc_module, key)

    @property
    def servicer(self):
        grpc_module = self.grpc_module

        for key in dir(grpc_module):
            if key.endswith("Servicer"):
                return getattr(self.grpc_module, key)

    @property
    def proto_module(self):
        return PROTO_MODULES[self]

    @property
    def options(self):
        return [
            ("grpc.max_receive_message_length", MAX_RECEIVE_MESSAGE_SIZE),
        ]


GRPC_MODULES = {
    Service.ADMIN: admin_grpc,
    Service.AUTH: auth_grpc,
    Service.DEBUG: debug_grpc,
    Service.ENTERPRISE: enterprise_grpc,
    Service.HEALTH: health_grpc,
    Service.IDENTITY: identity_grpc,
    Service.LICENSE: license_grpc,
    Service.PFS: pfs_grpc,
    Service.PPS: pps_grpc,
    Service.TRANSACTION: transaction_grpc,
    Service.VERSION: version_grpc,
}

PROTO_MODULES = {
    Service.ADMIN: admin_proto,
    Service.AUTH: auth_proto,
    Service.DEBUG: debug_proto,
    Service.ENTERPRISE: enterprise_proto,
    Service.HEALTH: health_proto,
    Service.IDENTITY: identity_proto,
    Service.LICENSE: license_proto,
    Service.PFS: pfs_proto,
    Service.PPS: pps_proto,
    Service.TRANSACTION: transaction_proto,
    Service.VERSION: version_proto,
}
