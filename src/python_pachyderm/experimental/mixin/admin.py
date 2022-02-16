from ..proto.v2.admin_v2 import ClusterInfo, ApiStub as _AdminApiStub
from . import _synchronizer


@_synchronizer
class AdminApi(_synchronizer(_AdminApiStub)):
    """A mixin for admin-related functionality."""

    async def inspect_cluster(self) -> ClusterInfo:
        """Inspects a cluster.

        Returns
        -------
        admin_proto.ClusterInfo
            A protobuf object with info on the cluster.
        """
        return await super().inspect_cluster()
