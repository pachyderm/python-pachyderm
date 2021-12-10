from typing import List, Tuple

import grpc
from google.protobuf import empty_pb2

from python_pachyderm.proto.v2.admin import admin_pb2, admin_pb2_grpc


class AdminMixin:
    """A mixin for admin-related functionality."""

    _channel: grpc.Channel
    _metadata: List[Tuple[str, str]]

    def __init__(self):
        self.__stub = admin_pb2_grpc.APIStub(self._channel)

    def inspect_cluster(self) -> admin_pb2.ClusterInfo:
        """Inspects a cluster.

        Returns
        -------
        admin_proto.ClusterInfo
            A protobuf object with info on the cluster.
        """
        message = empty_pb2.Empty()
        return self.__stub.InspectCluster(message, metadata=self._metadata)
