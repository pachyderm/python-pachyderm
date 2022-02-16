from ..proto.v2.enterprise_v2 import (
    ApiStub as _EnterpriseApiStub,
    GetActivationCodeResponse,
    GetStateResponse,
    State,
)
from . import _synchronizer


@_synchronizer
class EnterpriseApi(_synchronizer(_EnterpriseApiStub)):
    """A mixin for enterprise-related functionality."""

    async def activate(self, license_server: str, id: str, secret: str) -> None:
        """Activates enterprise by registering with a license server.

        Parameters
        ----------
        license_server : str
            The Pachyderm Enterprise Server to register with.
        id : str
            The unique ID for this cluster.
        secret : str
            The secret for registering this cluster.
        """
        await super().activate(license_server=license_server, id=id, secret=secret)

    async def get_state(self) -> GetStateResponse:
        """Gets the current enterprise state of the cluster.

        Returns
        -------
        enterprise_proto.GetStateResponse
            A protobuf object that returns a state enum, info on the token,
            and an empty activation code.
        """
        return await super().get_state()

    async def deactivate(self) -> None:
        """Deactivates enterprise."""
        await super().deactivate()

    async def get_activation_code(self) -> GetActivationCodeResponse:
        """Returns the enterprise code used to activate Pachyderm Enterprise in
        this cluster.

        Returns
        -------
        enterprise_proto.GetActivationCodeResponse
            A protobuf object that returns a state enum, info on the token,
            and the activation code.
        """
        return await super().get_activation_code()
