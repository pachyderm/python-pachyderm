from grpc_health.v1 import health_pb2 as health_proto
from python_pachyderm.experimental.proto.v2 import admin_v2 as admin_proto
from python_pachyderm.experimental.proto.v2 import auth_v2 as auth_proto
from python_pachyderm.experimental.proto.v2 import debug_v2 as debug_proto
from python_pachyderm.experimental.proto.v2 import enterprise_v2 as enterprise_proto
from python_pachyderm.experimental.proto.v2 import identity_v2 as identity_proto
from python_pachyderm.experimental.proto.v2 import license_v2 as license_proto
from python_pachyderm.experimental.proto.v2 import pfs_v2 as pfs_proto
from python_pachyderm.experimental.proto.v2 import pps_v2 as pps_proto
from python_pachyderm.experimental.proto.v2 import transaction_v2 as transaction_proto
from python_pachyderm.experimental.proto.v2 import versionpb_v2 as version_proto

from python_pachyderm.service import Service


BP_MODULES = {
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
