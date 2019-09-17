from python_pachyderm._proto.version.versionpb.version_pb2_grpc import (
    google_dot_protobuf_dot_empty__pb2 as pb_empty,
    APIStub as VersionStub,
    grpc
)
from contextlib import closing
from python_pachyderm.util import get_address

def get_remote_version(host=None, port=None):
    with closing(grpc.insecure_channel(get_address(host, port))) as channel:
        stub = VersionStub(channel)
        version = stub.GetVersion(pb_empty.Empty())
        return version
