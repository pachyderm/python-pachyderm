from typing import Iterator
from python_pachyderm.service import Service, debug_proto
from google.protobuf import duration_pb2


class DebugMixin:
    """A mixin for debug-related functionality."""

    def dump(
        self, filter: debug_proto.Filter = None, limit: int = None
    ) -> Iterator[bytes]:
        """Gets a debug dump.

        Parameters
        ----------
        filter : debug_proto.Filter, optional
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
        >>> for b in client.dump(debug_proto.Filter(pipeline=pps_proto.Pipeline(name="foo"))):
        >>>     print(b)

        .. # noqa: W505
        """
        res = self._req(Service.DEBUG, "Dump", filter=filter, limit=limit)
        for item in res:
            yield item.value

    def profile_cpu(
        self, duration: duration_pb2.Duration, filter: debug_proto.Filter = None
    ) -> Iterator[bytes]:
        """Gets a CPU profile.

        Parameters
        ----------
        duration : duration_pb2.Duration
            A google protobuf duration object indicating how long the profile
            should run for.
        filter : debug_proto.Filter, optional
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
        profile = debug_proto.Profile(name="cpu", duration=duration)
        res = self._req(Service.DEBUG, "Profile", profile=profile, filter=filter)
        for item in res:
            yield item.value

    def binary(self, filter: debug_proto.Filter = None) -> Iterator[bytes]:
        """Gets the pachd binary.

        Parameters
        ----------
        filter : debug_proto.Filter, optional
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
        res = self._req(Service.DEBUG, "Binary", filter=filter)
        for item in res:
            yield item.value
