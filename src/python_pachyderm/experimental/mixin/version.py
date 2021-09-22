from python_pachyderm.service import Service, version_proto
from google.protobuf import empty_pb2


class VersionMixin:
    """A mixin for version-related functionality."""

    def get_remote_version(self) -> version_proto.Version:
        """Gets version of Pachyderm server.

        Returns
        -------
        version_proto.Version
            A protobuf object with info on the pachd version.
        """
        return self._req(
            Service.VERSION,
            "GetVersion",
            req=empty_pb2.Empty(),
        )
