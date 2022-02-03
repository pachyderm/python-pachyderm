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
