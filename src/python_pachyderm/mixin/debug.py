from typing import Iterator

import grpc
from google.protobuf import duration_pb2

from python_pachyderm.proto.v2.debug import debug_pb2, debug_pb2_grpc


class DebugMixin:
    """A mixin for debug-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = debug_pb2_grpc.DebugStub(self._channel)
        super().__init__()

    def dump(
        self, filter: debug_pb2.Filter = None, limit: int = None
    ) -> Iterator[bytes]:
        """Gets a debug dump.

        Parameters
        ----------
        filter : debug_pb2.Filter, optional
            A protobuf object that filters what info is returned. Is one of
            pachd bool, pipeline protobuf, or worker protobuf.
        limit : int, optional
            Sets a limit to how many commits, jobs, pipelines, etc. are
            returned.

        Yields
        -------
        bytes
            The debug dump as a sequence of bytearrays.

        Examples
        --------
        >>> for b in client.dump(debug_pb2.Filter(pipeline=pps_pb2.Pipeline(name="foo"))):
        >>>     print(b)

        .. # noqa: W505
        """
        message = debug_pb2.DumpRequest(filter=filter, limit=limit)
        for item in self.__stub.Dump(message):
            yield item.value

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
