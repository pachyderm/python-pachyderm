import grpc
from google.protobuf import empty_pb2

from python_pachyderm.proto.v2.version.versionpb import version_pb2, version_pb2_grpc


class VersionMixin:
    """A mixin for version-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = version_pb2_grpc.APIStub(self._channel)
        super().__init__()

    def get_remote_version(self) -> version_pb2.Version:
        """Gets version of Pachyderm server.

        Returns
        -------
        version_pb2.Version
            A protobuf object with info on the pachd version.
        """
        message = empty_pb2.Empty()
        return self.__stub.GetVersion(message)
