from python_pachyderm.proto.v2.admin import admin_pb2_grpc as admin_grpc
from python_pachyderm.service import Service


class AdminMixin:
    def inspect_cluster(self):
        """
        Inspects a cluster. Returns a `ClusterInfo` object.
        """
        return self._req(
            Service.ADMIN,
            "InspectCluster",
            req=admin_grpc.google_dot_protobuf_dot_empty__pb2.Empty(),
        )
