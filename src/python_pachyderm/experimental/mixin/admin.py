from python_pachyderm.service import Service, admin_proto
from google.protobuf import empty_pb2


class AdminMixin:
    """A mixin for admin-related functionality."""

    def inspect_cluster(self) -> admin_proto.ClusterInfo:
        """Inspects a cluster.

        Returns
        -------
        admin_proto.ClusterInfo
            A protobuf object with info on the cluster.
        """
        return self._req(
            Service.ADMIN,
            "InspectCluster",
            req=empty_pb2.Empty(),
        )
