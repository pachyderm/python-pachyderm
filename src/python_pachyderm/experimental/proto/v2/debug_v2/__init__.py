# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: python_pachyderm/proto/v2/debug/debug.proto
# plugin: python-betterproto
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterator, Dict

import betterproto
from betterproto.grpc.grpclib_server import ServiceBase
import grpclib


@dataclass(eq=False, repr=False)
class ProfileRequest(betterproto.Message):
    profile: "Profile" = betterproto.message_field(1)
    filter: "Filter" = betterproto.message_field(2)


@dataclass(eq=False, repr=False)
class Profile(betterproto.Message):
    name: str = betterproto.string_field(1)
    duration: timedelta = betterproto.message_field(2)


@dataclass(eq=False, repr=False)
class Filter(betterproto.Message):
    pachd: bool = betterproto.bool_field(1, group="filter")
    pipeline: "_pps_v2__.Pipeline" = betterproto.message_field(2, group="filter")
    worker: "Worker" = betterproto.message_field(3, group="filter")


@dataclass(eq=False, repr=False)
class Worker(betterproto.Message):
    pod: str = betterproto.string_field(1)
    redirected: bool = betterproto.bool_field(2)


@dataclass(eq=False, repr=False)
class BinaryRequest(betterproto.Message):
    filter: "Filter" = betterproto.message_field(1)


@dataclass(eq=False, repr=False)
class DumpRequest(betterproto.Message):
    filter: "Filter" = betterproto.message_field(1)
    # Limit sets the limit for the number of commits / jobs that are returned for
    # each repo / pipeline in the dump.
    limit: int = betterproto.int64_field(2)


class DebugStub(betterproto.ServiceStub):
    async def profile(
        self, *, profile: "Profile" = None, filter: "Filter" = None
    ) -> AsyncIterator["betterproto_lib_google_protobuf.BytesValue"]:

        request = ProfileRequest()
        if profile is not None:
            request.profile = profile
        if filter is not None:
            request.filter = filter

        async for response in self._unary_stream(
            "/debug_v2.Debug/Profile",
            request,
            betterproto_lib_google_protobuf.BytesValue,
        ):
            yield response

    async def binary(
        self, *, filter: "Filter" = None
    ) -> AsyncIterator["betterproto_lib_google_protobuf.BytesValue"]:

        request = BinaryRequest()
        if filter is not None:
            request.filter = filter

        async for response in self._unary_stream(
            "/debug_v2.Debug/Binary",
            request,
            betterproto_lib_google_protobuf.BytesValue,
        ):
            yield response

    async def dump(
        self, *, filter: "Filter" = None, limit: int = 0
    ) -> AsyncIterator["betterproto_lib_google_protobuf.BytesValue"]:

        request = DumpRequest()
        if filter is not None:
            request.filter = filter
        request.limit = limit

        async for response in self._unary_stream(
            "/debug_v2.Debug/Dump",
            request,
            betterproto_lib_google_protobuf.BytesValue,
        ):
            yield response


class DebugBase(ServiceBase):
    async def profile(
        self, profile: "Profile", filter: "Filter"
    ) -> AsyncIterator["betterproto_lib_google_protobuf.BytesValue"]:
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def binary(
        self, filter: "Filter"
    ) -> AsyncIterator["betterproto_lib_google_protobuf.BytesValue"]:
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def dump(
        self, filter: "Filter", limit: int
    ) -> AsyncIterator["betterproto_lib_google_protobuf.BytesValue"]:
        raise grpclib.GRPCError(grpclib.const.Status.UNIMPLEMENTED)

    async def __rpc_profile(self, stream: grpclib.server.Stream) -> None:
        request = await stream.recv_message()

        request_kwargs = {
            "profile": request.profile,
            "filter": request.filter,
        }

        await self._call_rpc_handler_server_stream(
            self.profile,
            stream,
            request_kwargs,
        )

    async def __rpc_binary(self, stream: grpclib.server.Stream) -> None:
        request = await stream.recv_message()

        request_kwargs = {
            "filter": request.filter,
        }

        await self._call_rpc_handler_server_stream(
            self.binary,
            stream,
            request_kwargs,
        )

    async def __rpc_dump(self, stream: grpclib.server.Stream) -> None:
        request = await stream.recv_message()

        request_kwargs = {
            "filter": request.filter,
            "limit": request.limit,
        }

        await self._call_rpc_handler_server_stream(
            self.dump,
            stream,
            request_kwargs,
        )

    def __mapping__(self) -> Dict[str, grpclib.const.Handler]:
        return {
            "/debug_v2.Debug/Profile": grpclib.const.Handler(
                self.__rpc_profile,
                grpclib.const.Cardinality.UNARY_STREAM,
                ProfileRequest,
                betterproto_lib_google_protobuf.BytesValue,
            ),
            "/debug_v2.Debug/Binary": grpclib.const.Handler(
                self.__rpc_binary,
                grpclib.const.Cardinality.UNARY_STREAM,
                BinaryRequest,
                betterproto_lib_google_protobuf.BytesValue,
            ),
            "/debug_v2.Debug/Dump": grpclib.const.Handler(
                self.__rpc_dump,
                grpclib.const.Cardinality.UNARY_STREAM,
                DumpRequest,
                betterproto_lib_google_protobuf.BytesValue,
            ),
        }


from .. import pps_v2 as _pps_v2__
import betterproto.lib.google.protobuf as betterproto_lib_google_protobuf
