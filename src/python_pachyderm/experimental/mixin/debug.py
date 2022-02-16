import datetime
from typing import Iterator

from ..proto.v2.debug_v2 import (
    DebugStub as _DebugStub,
    Filter,
    Profile,
)
from ..proto.v2.pps_v2 import Pipeline
from . import _synchronizer

# bp_to_pb: datetime.deltatime -> duration_pb2.Duration


@_synchronizer
class DebugApi(_synchronizer(_DebugStub)):
    """A mixin for debug-related functionality."""

    async def dump(self, filter: Filter = None, limit: int = None) -> Iterator[bytes]:
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
        >>> for b in client.dump(Filter(pipeline=Pipeline(name="foo"))):
        >>>     print(b)

        .. # noqa: W505
        """
        async for item in super().dump(filter=filter, limit=limit):
            yield item.value

    async def profile_cpu(
        self, duration: datetime.timedelta, filter: Filter = None
    ) -> Iterator[bytes]:
        """Gets a CPU profile.

        Parameters
        ----------
        duration : datetime.timedelta
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
        >>> for b in client.profile_cpu(datetime.timedelta(seconds=1)):
        >>>     print(b)
        """

        profile = Profile(name="cpu", duration=duration)
        async for item in super().profile(profile=profile, filter=filter):
            yield item.value

    async def binary(self, filter: Filter = None) -> Iterator[bytes]:
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
        async for item in super().binary(filter=filter):
            yield item.value
