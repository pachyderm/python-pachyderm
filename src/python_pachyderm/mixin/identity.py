from typing import List
from python_pachyderm.service import Service, identity_proto


class IdentityMixin:
    """A mixin for identity-related functionality."""

    def set_identity_server_config(
        self, config: identity_proto.IdentityServerConfig
    ) -> None:
        """Configure the embedded identity server.

        Parameters
        ----------
        config : identity_proto.IdentityServerConfig
            A protobuf object that is the configuration for the identity web
            server.
        """
        self._req(Service.IDENTITY, "SetIdentityServerConfig", config=config)

    def get_identity_server_config(self) -> identity_proto.IdentityServerConfig:
        """Get the embedded identity server configuration.

        Returns
        -------
        identity_proto.IdentityServerConfig
            A protobuf object that holds configuration info (issuer and ID
            token expiry) for the identity web server.
        """
        return self._req(Service.IDENTITY, "GetIdentityServerConfig").config

    def create_idp_connector(self, connector: identity_proto.IDPConnector) -> None:
        """Create an IDP connector in the identity server.

        Parameters
        ----------
        connector : identity_proto.IDPConnector
            A protobuf object that represents a connection to an identity
            provider.
        """
        self._req(Service.IDENTITY, "CreateIDPConnector", connector=connector)

    def list_idp_connectors(self) -> List[identity_proto.IDPConnector]:
        """List IDP connectors in the identity server.

        Returns
        -------
        List[identity_proto.IDPConnector]
            A list of probotuf objects that return info on the connector ID,
            name, type, config version, and configuration of the upstream IDP
            connector.
        """
        return self._req(Service.IDENTITY, "ListIDPConnectors").connectors

    def update_idp_connector(self, connector: identity_proto.IDPConnector) -> None:
        """Update an IDP connector in the identity server.

        Parameters
        ----------
        connector : identity_proto.IDPConnector
            A protobuf object that represents a connection to an identity
            provider.
        """
        self._req(Service.IDENTITY, "UpdateIDPConnector", connector=connector)

    def get_idp_connector(self, id: str) -> identity_proto.IDPConnector:
        """Get an IDP connector in the identity server.

        Parameters
        ----------
        id : str
            The connector ID.

        Returns
        -------
        identity_proto.IDPConnector
            A protobuf object that returns info on the connector ID, name,
            type, config version, and configuration of the upstream IDP
            connector.
        """
        return self._req(Service.IDENTITY, "GetIDPConnector", id=id).connector

    def delete_idp_connector(self, id: str) -> None:
        """Delete an IDP connector in the identity server.

        Parameters
        ----------
        id : str
            The connector ID.
        """
        self._req(Service.IDENTITY, "DeleteIDPConnector", id=id)

    def create_oidc_client(
        self, client: identity_proto.OIDCClient
    ) -> identity_proto.OIDCClient:
        """Create an OIDC client in the identity server.

        Parameters
        ----------
        client : identity_proto.OIDCClient
            A protobuf object representing an OIDC client.

        Returns
        -------
        identity_proto.OIDCClient
            A protobuf object that returns a client with info on the client ID,
            name, secret, and lists of redirect URIs and trusted peers.
        """
        return self._req(Service.IDENTITY, "CreateOIDCClient", client=client).client

    def update_oidc_client(self, client: identity_proto.OIDCClient) -> None:
        """Update an OIDC client in the identity server.

        Parameters
        ----------
        client : identity_proto.OIDCClient
            A protobuf object representing an OIDC client.

        """
        return self._req(Service.IDENTITY, "UpdateOIDCClient", client=client)

    def get_oidc_client(self, id: str) -> identity_proto.OIDCClient:
        """Get an OIDC client in the identity server.

        Parameters
        ----------
        id : str
            The client ID.

        Returns
        -------
        identity_proto.OIDCClient
            A protobuf object that returns a client with info on the client ID,
            name, secret, and lists of redirect URIs and trusted peers.
        """
        return self._req(Service.IDENTITY, "GetOIDCClient", id=id).client

    def delete_oidc_client(self, id: str) -> None:
        """Delete an OIDC client in the identity server.

        Parameters
        ----------
        id : str
            The client ID.
        """
        self._req(Service.IDENTITY, "DeleteOIDCClient", id=id)

    def list_oidc_clients(self) -> List[identity_proto.OIDCClient]:
        """List OIDC clients in the identity server.

        Returns
        -------
        List[identity_proto.OIDCClient]
            A list of protobuf objects that return a client with info on the
            client ID, name, secret, and lists of redirect URIs and trusted
            peers.
        """
        return self._req(Service.IDENTITY, "ListOIDCClients").clients

    def delete_all_identity(self) -> None:
        """Delete all identity service information."""
        self._req(Service.IDENTITY, "DeleteAll")
