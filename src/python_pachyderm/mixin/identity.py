from typing import List

import grpc

from python_pachyderm.errors import AuthServiceNotActivated
from python_pachyderm.proto.v2.identity import identity_pb2, identity_pb2_grpc


class IdentityMixin:
    """A mixin for identity-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = identity_pb2_grpc.APIStub(self._channel)
        super().__init__()

    def set_identity_server_config(
        self, config: identity_pb2.IdentityServerConfig
    ) -> None:
        """Configure the embedded identity server.

        Parameters
        ----------
        config : identity_pb2.IdentityServerConfig
            A protobuf object that is the configuration for the identity web
            server.
        """
        message = identity_pb2.SetIdentityServerConfigRequest(config=config)
        self.__stub.SetIdentityServerConfig(message)

    def get_identity_server_config(self) -> identity_pb2.IdentityServerConfig:
        """Get the embedded identity server configuration.

        Returns
        -------
        identity_pb2.IdentityServerConfig
            A protobuf object that holds configuration info (issuer and ID
            token expiry) for the identity web server.
        """
        message = identity_pb2.GetIdentityServerConfigRequest()
        return self.__stub.GetIdentityServerConfig(message).config

    def create_idp_connector(self, connector: identity_pb2.IDPConnector) -> None:
        """Create an IDP connector in the identity server.

        Parameters
        ----------
        connector : identity_pb2.IDPConnector
            A protobuf object that represents a connection to an identity
            provider.
        """
        message = identity_pb2.CreateIDPConnectorRequest(connector=connector)
        self.__stub.CreateIDPConnector(message)

    def list_idp_connectors(self) -> List[identity_pb2.IDPConnector]:
        """List IDP connectors in the identity server.

        Returns
        -------
        List[identity_pb2.IDPConnector]
            A list of probotuf objects that return info on the connector ID,
            name, type, config version, and configuration of the upstream IDP
            connector.
        """
        message = identity_pb2.ListIDPConnectorsRequest()
        return self.__stub.ListIDPConnectors(message).connectors

    def update_idp_connector(self, connector: identity_pb2.IDPConnector) -> None:
        """Update an IDP connector in the identity server.

        Parameters
        ----------
        connector : identity_pb2.IDPConnector
            A protobuf object that represents a connection to an identity
            provider.
        """
        message = identity_pb2.UpdateIDPConnectorRequest(connector=connector)
        self.__stub.UpdateIDPConnector(message)

    def get_idp_connector(self, id: str) -> identity_pb2.IDPConnector:
        """Get an IDP connector in the identity server.

        Parameters
        ----------
        id : str
            The connector ID.

        Returns
        -------
        identity_pb2.IDPConnector
            A protobuf object that returns info on the connector ID, name,
            type, config version, and configuration of the upstream IDP
            connector.
        """
        message = identity_pb2.GetIDPConnectorRequest(id=id)
        return self.__stub.GetIDPConnector(message).connector

    def delete_idp_connector(self, id: str) -> None:
        """Delete an IDP connector in the identity server.

        Parameters
        ----------
        id : str
            The connector ID.
        """
        message = identity_pb2.DeleteIDPConnectorRequest(id=id)
        self.__stub.DeleteIDPConnector(message)

    def create_oidc_client(
        self, client: identity_pb2.OIDCClient
    ) -> identity_pb2.OIDCClient:
        """Create an OIDC client in the identity server.

        Parameters
        ----------
        client : identity_pb2.OIDCClient
            A protobuf object representing an OIDC client.

        Returns
        -------
        identity_pb2.OIDCClient
            A protobuf object that returns a client with info on the client ID,
            name, secret, and lists of redirect URIs and trusted peers.
        """
        message = identity_pb2.CreateOIDCClientRequest(client=client)
        return self.__stub.CreateOIDCClient(message).client

    def update_oidc_client(self, client: identity_pb2.OIDCClient) -> None:
        """Update an OIDC client in the identity server.

        Parameters
        ----------
        client : identity_pb2.OIDCClient
            A protobuf object representing an OIDC client.

        """
        message = identity_pb2.UpdateOIDCClientRequest(client=client)
        return self.__stub.UpdateOIDCClient(message)

    def get_oidc_client(self, id: str) -> identity_pb2.OIDCClient:
        """Get an OIDC client in the identity server.

        Parameters
        ----------
        id : str
            The client ID.

        Returns
        -------
        identity_pb2.OIDCClient
            A protobuf object that returns a client with info on the client ID,
            name, secret, and lists of redirect URIs and trusted peers.
        """
        message = identity_pb2.GetOIDCClientRequest(id=id)
        return self.__stub.GetOIDCClient(message).client

    def delete_oidc_client(self, id: str) -> None:
        """Delete an OIDC client in the identity server.

        Parameters
        ----------
        id : str
            The client ID.
        """
        message = identity_pb2.DeleteOIDCClientRequest(id=id)
        self.__stub.DeleteOIDCClient(message)

    def list_oidc_clients(self) -> List[identity_pb2.OIDCClient]:
        """List OIDC clients in the identity server.

        Returns
        -------
        List[identity_pb2.OIDCClient]
            A list of protobuf objects that return a client with info on the
            client ID, name, secret, and lists of redirect URIs and trusted
            peers.
        """
        message = identity_pb2.ListOIDCClientsRequest()
        return self.__stub.ListOIDCClients(message).clients

    def delete_all_identity(self) -> None:
        """Delete all identity service information.

        Raises
        ------
        AuthServiceNotActivated
        """
        message = identity_pb2.DeleteAllRequest()
        try:
            self.__stub.DeleteAll(message)
        except grpc.RpcError as err:
            raise AuthServiceNotActivated.try_from(err)
