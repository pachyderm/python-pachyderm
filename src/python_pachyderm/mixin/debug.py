from python_pachyderm.service import Service
from python_pachyderm.proto.debug import debug_pb2 as debug_proto


class DebugMixin:
    def dump(self, filter=None, limit=None):
        """
        Gets a debug dump. Yields byte arrays.

        Params:

        * `recursed`: An optional bool.
        * `filter`: An optional `Filter` object.
        * `limit`: An optional int, limiting the number of commits/jobs returned
          for each repo/pipeline in the dump
        """
        res = self._req(Service.DEBUG, "Dump", filter=filter, limit=limit)
        for item in res:
            yield item.value

    def profile_cpu(self, duration, filter=None):
        """
        Gets a CPU profile. Yields byte arrays.

        Params:

        * `duration`: A `Duration` object specifying how long to run the CPU
          profiler.
        * `filter`: An optional `Filter` object.
        """
        profile = debug_proto.Profile(name="cpu", duration=duration)
        res = self._req(Service.DEBUG, "Profile", profile=profile, filter=filter)
        for item in res:
            yield item.value

    def binary(self, filter=None):
        """
        Gets the pachd binary. Yields byte arrays.

        Params:

        * `filter`: An optional `Filter` object.
        """
        res = self._req(Service.DEBUG, "Binary", filter=filter)
        for item in res:
            yield item.value
