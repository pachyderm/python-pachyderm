# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from python_pachyderm.proto.v2.license import license_pb2 as python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2


class APIStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Activate = channel.unary_unary(
                '/license.API/Activate',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ActivateRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ActivateResponse.FromString,
                )
        self.GetActivationCode = channel.unary_unary(
                '/license.API/GetActivationCode',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.GetActivationCodeRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.GetActivationCodeResponse.FromString,
                )
        self.DeleteAll = channel.unary_unary(
                '/license.API/DeleteAll',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteAllRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteAllResponse.FromString,
                )
        self.AddCluster = channel.unary_unary(
                '/license.API/AddCluster',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.AddClusterRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.AddClusterResponse.FromString,
                )
        self.DeleteCluster = channel.unary_unary(
                '/license.API/DeleteCluster',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteClusterRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteClusterResponse.FromString,
                )
        self.ListClusters = channel.unary_unary(
                '/license.API/ListClusters',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ListClustersRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ListClustersResponse.FromString,
                )
        self.UpdateCluster = channel.unary_unary(
                '/license.API/UpdateCluster',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.UpdateClusterRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.UpdateClusterResponse.FromString,
                )
        self.Heartbeat = channel.unary_unary(
                '/license.API/Heartbeat',
                request_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.HeartbeatRequest.SerializeToString,
                response_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.HeartbeatResponse.FromString,
                )


class APIServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Activate(self, request, context):
        """Activate enables the license service by setting the enterprise activation
        code to serve.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetActivationCode(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteAll(self, request, context):
        """DeleteAll deactivates the server and removes all data.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def AddCluster(self, request, context):
        """CRUD operations for the pachds registered with this server.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteCluster(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListClusters(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UpdateCluster(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Heartbeat(self, request, context):
        """Heartbeat is the RPC registered pachds make to the license server
        to communicate their status and fetch updates.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_APIServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Activate': grpc.unary_unary_rpc_method_handler(
                    servicer.Activate,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ActivateRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ActivateResponse.SerializeToString,
            ),
            'GetActivationCode': grpc.unary_unary_rpc_method_handler(
                    servicer.GetActivationCode,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.GetActivationCodeRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.GetActivationCodeResponse.SerializeToString,
            ),
            'DeleteAll': grpc.unary_unary_rpc_method_handler(
                    servicer.DeleteAll,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteAllRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteAllResponse.SerializeToString,
            ),
            'AddCluster': grpc.unary_unary_rpc_method_handler(
                    servicer.AddCluster,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.AddClusterRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.AddClusterResponse.SerializeToString,
            ),
            'DeleteCluster': grpc.unary_unary_rpc_method_handler(
                    servicer.DeleteCluster,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteClusterRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteClusterResponse.SerializeToString,
            ),
            'ListClusters': grpc.unary_unary_rpc_method_handler(
                    servicer.ListClusters,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ListClustersRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ListClustersResponse.SerializeToString,
            ),
            'UpdateCluster': grpc.unary_unary_rpc_method_handler(
                    servicer.UpdateCluster,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.UpdateClusterRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.UpdateClusterResponse.SerializeToString,
            ),
            'Heartbeat': grpc.unary_unary_rpc_method_handler(
                    servicer.Heartbeat,
                    request_deserializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.HeartbeatRequest.FromString,
                    response_serializer=python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.HeartbeatResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'license.API', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class API(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Activate(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/Activate',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ActivateRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ActivateResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetActivationCode(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/GetActivationCode',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.GetActivationCodeRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.GetActivationCodeResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DeleteAll(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/DeleteAll',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteAllRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteAllResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def AddCluster(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/AddCluster',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.AddClusterRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.AddClusterResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DeleteCluster(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/DeleteCluster',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteClusterRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.DeleteClusterResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def ListClusters(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/ListClusters',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ListClustersRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.ListClustersResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UpdateCluster(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/UpdateCluster',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.UpdateClusterRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.UpdateClusterResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Heartbeat(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/license.API/Heartbeat',
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.HeartbeatRequest.SerializeToString,
            python__pachyderm_dot_proto_dot_v2_dot_license_dot_license__pb2.HeartbeatResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
