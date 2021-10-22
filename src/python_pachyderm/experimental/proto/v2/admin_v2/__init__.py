# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: python_pachyderm/proto/v2/admin/admin.proto
# plugin: python-betterproto
from dataclasses import dataclass
from typing import Dict

import betterproto
from betterproto.grpc.grpclib_server import ServiceBase
import grpclib


@dataclass(eq=False, repr=False)
class ClusterInfo(betterproto.Message):
    id: str = betterproto.string_field(1)
    deployment_id: str = betterproto.string_field(2)


class ApiStub(betterproto.ServiceStub):
    async def inspect_cluster(self) -> "ClusterInfo":

        request = betterproto_lib_google_protobuf.Empty()

        return await self._unary_unary(
            "/admin_v2.API/InspectCluster", request, ClusterInfo
        )


class ApiBase(ServiceBase):
    async def inspect_cluster(self) -> "ClusterInfo":
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def __rpc_inspect_cluster(self, stream: grpclib.server.Stream) -> None:
        request = await stream.recv_message()

        request_kwargs = {}

        response = await self.inspect_cluster(**request_kwargs)
        await stream.send_message(response)

    def __mapping__(self) -> Dict[str, grpclib.const.Handler]:
        return {
            "/admin_v2.API/InspectCluster": grpclib.const.Handler(
                self.__rpc_inspect_cluster,
                grpclib.const.Cardinality.UNARY_UNARY,
                betterproto_lib_google_protobuf.Empty,
                ClusterInfo,
            ),
        }


import betterproto.lib.google.protobuf as betterproto_lib_google_protobuf
