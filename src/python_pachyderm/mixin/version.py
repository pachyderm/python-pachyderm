from python_pachyderm.service import Service, version_proto
from google.protobuf import empty_pb2


class VersionMixin:
    def get_remote_version(self) -> version_proto.Version:
        return self._req(
            Service.VERSION,
            "GetVersion",
            req=empty_pb2.Empty(),
        )
