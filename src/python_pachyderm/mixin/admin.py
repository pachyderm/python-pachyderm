from python_pachyderm.service import Service, admin_proto
from google.protobuf import empty_pb2


class AdminMixin:
    def inspect_cluster(self) -> admin_proto.ClusterInfo:
        """
        Inspects a cluster. Returns a `ClusterInfo` object.
        """
        return self._req(
            Service.ADMIN,
            "InspectCluster",
            req=empty_pb2.Empty(),
        )
