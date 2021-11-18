from python_pachyderm.service import Service
from python_pachyderm.experimental.service import version_proto
from google.protobuf import empty_pb2
import betterproto.lib.google.protobuf as bp_proto

# bp_to_pb: bp_proto.Empty -> empty_pb2.Empty


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
            req=bp_proto.Empty(),
        )
