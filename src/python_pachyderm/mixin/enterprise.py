import grpc

from python_pachyderm.proto.v2.enterprise import enterprise_pb2, enterprise_pb2_grpc


class EnterpriseMixin:
    """A mixin for enterprise-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = enterprise_pb2_grpc.APIStub(self._channel)
        super().__init__()

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
        message = enterprise_pb2.ActivateRequest(
            license_server=license_server,
            id=id,
            secret=secret,
        )
        self.__stub.Activate(message)

    def get_enterprise_state(self) -> enterprise_pb2.GetStateResponse:
        """Gets the current enterprise state of the cluster.

        Returns
        -------
        enterprise_pb2.GetStateResponse
            A protobuf object that returns a state enum, info on the token,
            and an empty activation code.
        """
        message = enterprise_pb2.GetStateRequest()
        return self.__stub.GetState(message)

    def deactivate_enterprise(self) -> None:
        """Deactivates enterprise."""
        message = enterprise_pb2.DeactivateRequest()
        self.__stub.Deactivate(message)

    def get_activation_code(self) -> enterprise_pb2.GetActivationCodeResponse:
        """Returns the enterprise code used to activate Pachyderm Enterprise in
        this cluster.

        Returns
        -------
        enterprise_pb2.GetActivationCodeResponse
            A protobuf object that returns a state enum, info on the token,
            and the activation code.
        """
        message = enterprise_pb2.GetActivationCodeRequest()
        return self.__stub.GetActivationCode(message)
