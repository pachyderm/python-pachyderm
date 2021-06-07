# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from python_pachyderm.proto.v2.version.versionpb import version_pb2 as python__pachyderm_dot_proto_dot_v2_dot_version_dot_versionpb_dot_version__pb2


class APIStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.GetVersion = channel.unary_unary(
                '/versionpb.API/GetVersion',
                request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_version_dot_versionpb_dot_version__pb2.Version.FromString,
                )


class APIServicer(object):
    """Missing associated documentation comment in .proto file."""

    def GetVersion(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_APIServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'GetVersion': grpc.unary_unary_rpc_method_handler(
                    servicer.GetVersion,
                    request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_version_dot_versionpb_dot_version__pb2.Version.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'versionpb.API', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class API(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def GetVersion(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/versionpb.API/GetVersion',
            google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_version_dot_versionpb_dot_version__pb2.Version.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)