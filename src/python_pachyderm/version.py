from python_pachyderm.proto.version.versionpb import version_pb2_grpc as version_grpc


class VersionMixin:
    @property
    def _version_stub(self):
        if not hasattr(self, "__version_stub"):
            self.__version_stub = self._create_stub(version_grpc)
        return self.__version_stub

    def get_remote_version(self):
        req = version_grpc.google_dot_protobuf_dot_empty__pb2.Empty()
        return self._version_stub.GetVersion(req, metadata=self.metadata)
