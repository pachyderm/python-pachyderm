from python_pachyderm.service import Service
from python_pachyderm.proto.debug import debug_pb2 as debug_proto


class DebugMixin:
    def dump(self, filter=None, limit=None):
        """Gets a debug dump. Yields byte arrays.

        Parameters
        ----------
        filter : Filter protobuf, optional
            An optional `Filter` object.
        limit : int, optional
            Limits the number of commits/jobs returned for each repo/pipeline
            in the dump
        """
        res = self._req(Service.DEBUG, "Dump", filter=filter, limit=limit)
        for item in res:
            yield item.value

    def profile_cpu(self, duration, filter=None):
        """Gets a CPU profile. Yields byte arrays.

        Parameters
        ----------
        duration : Duration protobuf
            A ``Duration`` object specifying how long to run the CPU profiler.
        filter : Filter protobuf, optional
            An optional `Filter` object.
        """
        profile = debug_proto.Profile(name="cpu", duration=duration)
        res = self._req(Service.DEBUG, "Profile", profile=profile, filter=filter)
        for item in res:
            yield item.value

    def binary(self, filter=None):
        """Gets the pachd binary. Yields byte arrays.

        Parameters
        ----------
        filter : Filter protobuf, optional
            An optional `Filter` object.
        """
        res = self._req(Service.DEBUG, "Binary", filter=filter)
        for item in res:
            yield item.value
