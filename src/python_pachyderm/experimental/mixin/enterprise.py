from python_pachyderm.service import Service
from python_pachyderm.experimental.service import enterprise_proto


class EnterpriseMixin:
    """A mixin for enterprise-related functionality."""

    def activate_enterprise(self, license_server: str, id: str, secret: str) -> None:
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
        self._req(
            Service.ENTERPRISE,
            "Activate",
            license_server=license_server,
            id=id,
            secret=secret,
        )

    def get_enterprise_state(self) -> enterprise_proto.GetStateResponse:
        """Gets the current enterprise state of the cluster.

        Returns
        -------
        enterprise_proto.GetStateResponse
            A protobuf object that returns a state enum, info on the token,
            and an empty activation code.
        """
        return self._req(Service.ENTERPRISE, "GetState")

    def deactivate_enterprise(self) -> None:
        """Deactivates enterprise."""
        self._req(Service.ENTERPRISE, "Deactivate")

    def get_activation_code(self) -> enterprise_proto.GetActivationCodeResponse:
        """Returns the enterprise code used to activate Pachdyerm Enterprise in
        this cluster.

        Returns
        -------
        enterprise_proto.GetActivationCodeResponse
            A protobuf object that returns a state enum, info on the token,
            and the activation code.
        """
        return self._req(Service.ENTERPRISE, "GetActivationCode")

    def pause_enterprise(self) -> None:
        """Pauses the cluster."""
        self._req(Service.ENTERPRISE, "Pause")

    def unpause_enterprise(self) -> None:
        """Unpauses the cluster."""
        self._req(Service.ENTERPRISE, "Unpause")

    def get_pause_status(self) -> enterprise_proto.PauseStatusResponse:
        """Gets the pause status of the cluster.

        Returns
        -------
        enterprise_proto.PauseStatusResponse
            A protobuf object that returns a status enum.
        """
        return self._req(Service.ENTERPRISE, "PauseStatus")
