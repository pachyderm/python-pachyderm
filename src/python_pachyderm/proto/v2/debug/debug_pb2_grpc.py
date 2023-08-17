# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from google.protobuf import wrappers_pb2 as google_dot_protobuf_dot_wrappers__pb2
from python_pachyderm.proto.v2.debug import debug_pb2 as python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2


class DebugStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Profile = channel.unary_stream(
                '/debug_v2.Debug/Profile',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.ProfileRequest.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
                )
        self.Binary = channel.unary_stream(
                '/debug_v2.Debug/Binary',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.BinaryRequest.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
                )
        self.Dump = channel.unary_stream(
                '/debug_v2.Debug/Dump',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpRequest.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
                )
        self.SetLogLevel = channel.unary_unary(
                '/debug_v2.Debug/SetLogLevel',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.SetLogLevelRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.SetLogLevelResponse.FromString,
                )
        self.GetDumpV2Template = channel.unary_unary(
                '/debug_v2.Debug/GetDumpV2Template',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.GetDumpV2TemplateRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.GetDumpV2TemplateResponse.FromString,
                )
        self.DumpV2 = channel.unary_stream(
                '/debug_v2.Debug/DumpV2',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpV2Request.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpChunk.FromString,
                )


class DebugServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Profile(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Binary(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Dump(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SetLogLevel(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetDumpV2Template(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DumpV2(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_DebugServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Profile': grpc.unary_stream_rpc_method_handler(
                    servicer.Profile,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.ProfileRequest.FromString,
                    response_serializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.SerializeToString,
            ),
            'Binary': grpc.unary_stream_rpc_method_handler(
                    servicer.Binary,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.BinaryRequest.FromString,
                    response_serializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.SerializeToString,
            ),
            'Dump': grpc.unary_stream_rpc_method_handler(
                    servicer.Dump,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpRequest.FromString,
                    response_serializer=google_dot_protobuf_dot_wrappers__pb2.BytesValue.SerializeToString,
            ),
            'SetLogLevel': grpc.unary_unary_rpc_method_handler(
                    servicer.SetLogLevel,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.SetLogLevelRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.SetLogLevelResponse.SerializeToString,
            ),
            'GetDumpV2Template': grpc.unary_unary_rpc_method_handler(
                    servicer.GetDumpV2Template,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.GetDumpV2TemplateRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.GetDumpV2TemplateResponse.SerializeToString,
            ),
            'DumpV2': grpc.unary_stream_rpc_method_handler(
                    servicer.DumpV2,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpV2Request.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpChunk.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'debug_v2.Debug', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class Debug(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Profile(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/debug_v2.Debug/Profile',
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.ProfileRequest.SerializeToString,
            google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Binary(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/debug_v2.Debug/Binary',
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.BinaryRequest.SerializeToString,
            google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Dump(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/debug_v2.Debug/Dump',
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpRequest.SerializeToString,
            google_dot_protobuf_dot_wrappers__pb2.BytesValue.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SetLogLevel(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/debug_v2.Debug/SetLogLevel',
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.SetLogLevelRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.SetLogLevelResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetDumpV2Template(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/debug_v2.Debug/GetDumpV2Template',
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.GetDumpV2TemplateRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.GetDumpV2TemplateResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DumpV2(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/debug_v2.Debug/DumpV2',
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpV2Request.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_debug_dot_debug__pb2.DumpChunk.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
