from typing import Iterator, List

import grpc
from google.protobuf import duration_pb2

from python_pachyderm.proto.v2.debug import debug_pb2, debug_pb2_grpc
from python_pachyderm.proto.v2.pps import pps_pb2


class DebugMixin:
    """A mixin for debug-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = debug_pb2_grpc.DebugStub(self._channel)
        super().__init__()

    def dump(
        self,
        system: debug_pb2.System = None,
        pipelines: List[pps_pb2.Pipeline] = None,
        input_repos: bool = False,
        timeout: duration_pb2.Duration = None,
    ) -> Iterator[debug_pb2.DumpChunk]:
        """Collect a standard set of debugging information using the DumpV2 API
          rather than the now deprecated Dump API.

        This method is intended to be used in tandem with the
          `debug.get_dump_v2_template` endpoint. However, if no system or pipelines
          are specified then this call will automatically be performed for the user.

        If no system or pipelines are specified, then debug information for all
          systems and pipelines will be returned.

        Parameters
        ----------
        system : debug_pb2.System, optional
            A protobuf object that filters what info is returned.
        pipelines : List[pps_pb2.Pipeline], optional
            A list of pipelines from which to collect debug information.
        input_repos : bool
            Whether to collect debug information for input repos. Default: False
        timeout : duration_pb2.Duration, optional
            Duration until timeout occurs. Default is no timeout.

        Yields
        -------
        debug_pb2.DumpChunk
            Chunks of the debug dump

        Examples
        --------
        >>> for b in client.dump():
        >>>     print(b)

        .. # noqa: W505
        """
        message = debug_pb2.DumpV2Request(
            system=system,
            pipelines=pipelines or [],
        )
        if system is None and not pipelines:
            message = self.get_dump_template()
        if input_repos:
            message.input_repos = input_repos
        if timeout:
            message.timeout = timeout
        for item in self.__stub.DumpV2(message):
            yield item

    def get_dump_template(self, filters: List[str] = None) -> debug_pb2.DumpV2Request:
        """Generate a template request to be used by the DumpV2 API.

        Parameters
        ----------
        filters : List[str], optional
            No supported filters - this argument has no effect.

        Returns
        -------
        debug_pb2.DumpV2Request
            The request that can be sent to the DumpV2 API.

        .. # noqa: W505
        """
        message = debug_pb2.GetDumpV2TemplateRequest(filters=filters or [])
        return self.__stub.GetDumpV2Template(message).request

    def profile_cpu(
        self, duration: duration_pb2.Duration, filter: debug_pb2.Filter = None
    ) -> Iterator[bytes]:
        """Gets a CPU profile.

        Parameters
        ----------
        duration : duration_pb2.Duration
            A google protobuf duration object indicating how long the profile
            should run for.
        filter : debug_pb2.Filter, optional
            A protobuf object that filters what info is returned. Is one of
            pachd bool, pipeline protobuf, or worker protobuf.

        Yields
        -------
        bytes
            The cpu profile as a sequence of bytearrays.

        Examples
        --------
        >>> for b in client.profile_cpu(duration_pb2.Duration(seconds=1)):
        >>>     print(b)
        """
        message = debug_pb2.ProfileRequest(
            filter=filter,
            profile=debug_pb2.Profile(name="cpu", duration=duration),
        )
        for item in self.__stub.Profile(message):
            yield item.value

    def binary(self, filter: debug_pb2.Filter = None) -> Iterator[bytes]:
        """Gets the pachd binary.

        Parameters
        ----------
        filter : debug_pb2.Filter, optional
            A protobuf object that filters what info is returned. Is one of
            pachd bool, pipeline protobuf, or worker protobuf.

        Yields
        -------
        bytes
            The pachd binary as a sequence of bytearrays.

        Examples
        --------
        >>> for b in client.binary():
        >>>     print(b)
        """
        message = debug_pb2.BinaryRequest(filter=filter)
        for item in self.__stub.Binary(message):
            yield item.value

    def set_log_level(
        self,
        pachyderm_level: debug_pb2.SetLogLevelRequest.LogLevel = None,
        grpc_level: debug_pb2.SetLogLevelRequest.LogLevel = None,
        duration: duration_pb2.Duration = None,
        recurse: bool = True,
    ) -> debug_pb2.SetLogLevelResponse:
        """Sets the logging level of either pachyderm or grpc.
        Note: Only one level can be set at a time. If you are attempting to set
          multiple logging levels you must do so with multiple calls.

        Parameters
        ----------
        pachyderm_level: debug_pb2.SetLogLevelRequest.LogLevel, oneof
            The desired pachyderm logging level.
        grpc_level: debug_pb2.SetLogLevelRequest.LogLevel, oneof
            The desired grpc logging level.
        duration: duration_pb2.Duration, optional
            How long to log at the non-default level. (default 5m0s)
        recurse: bool
            Set the log level on all pachyderm pods; if false, only the pachd
            that handles this RPC. (default true)
        """
        message = debug_pb2.SetLogLevelRequest(duration=duration, recurse=recurse)
        message.pachyderm = pachyderm_level
        message.grpc = grpc_level
        return self.__stub.SetLogLevel(message)
