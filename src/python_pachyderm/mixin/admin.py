import grpc
from google.protobuf import empty_pb2

from python_pachyderm.proto.v2.admin import admin_pb2, admin_pb2_grpc


class AdminMixin:
    """A mixin for admin-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = admin_pb2_grpc.APIStub(self._channel)
        super().__init__()

    # TODO: This method should auto-populate it's message with information about
    #   the version of this package that is making the call. This is to allow
    #   the cluster to emit warnings about incompatible versions.
    #  This is not currently feasible to implement until we have better coupling
    #    with the versions of the core product.
    def inspect_cluster(self) -> admin_pb2.ClusterInfo:
        """Inspects a cluster.

        Returns
        -------
        admin_pb2.ClusterInfo
            A protobuf object with info on the cluster.
        """
        message = empty_pb2.Empty()
        return self.__stub.InspectCluster(message)
