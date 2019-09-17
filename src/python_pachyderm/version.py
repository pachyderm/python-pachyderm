from python_pachyderm._proto.version.versionpb.version_pb2 import *
from python_pachyderm._proto.version.versionpb import version_pb2_grpc as grpc

from contextlib import closing
from python_pachyderm.util import get_address


def get_remote_version(host=None, port=None):
    with closing(grpc.grpc.insecure_channel(get_address(host, port))) as channel:
        stub = grpc.APIStub(channel)
        version = stub.GetVersion(grpc.google_dot_protobuf_dot_empty__pb2.Empty())
        return version
