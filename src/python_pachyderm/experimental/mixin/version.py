from ..proto.v2.versionpb_v2 import ApiStub as _VersionApiStub, Version
from . import _synchronizer


@_synchronizer
class VersionApi(_synchronizer(_VersionApiStub)):
    """A mixin for version-related functionality."""

    async def get_remote_version(self) -> Version:
        """Gets version of Pachyderm server.

        Returns
        -------
        version_proto.Version
            A protobuf object with info on the pachd version.
        """
        return await self.get_version()
