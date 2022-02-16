from typing import List

import grpc

from python_pachyderm.errors import AuthServiceNotActivated
from ..proto.v2.identity_v2 import (
    ApiStub as _IdentityApiStub,
    IdentityServerConfig,
    IdpConnector,
    OidcClient,
)
from . import _synchronizer

# bp_to_pb: OidcClient -> OIDCClient, IdpConnector -> IDPConnector


@_synchronizer
class IdentityApi(_synchronizer(_IdentityApiStub)):
    """A mixin for identity-related functionality."""

    async def set_identity_server_config(self, config: IdentityServerConfig) -> None:
        """Configure the embedded identity server.

        Parameters
        ----------
        config : identity_proto.IdentityServerConfig
            A protobuf object that is the configuration for the identity web
            server.
        """
        await super().set_identity_server_config(config=config)

    async def get_identity_server_config(self) -> IdentityServerConfig:
        """Get the embedded identity server configuration.

        Returns
        -------
        identity_proto.IdentityServerConfig
            A protobuf object that holds configuration info (issuer and ID
            token expiry) for the identity web server.
        """
        response = await super().get_identity_server_config()
        return response.config

    async def create_idp_connector(self, connector: IdpConnector) -> None:
        """Create an IDP connector in the identity server.

        Parameters
        ----------
        connector : identity_proto.IdpConnector
            A protobuf object that represents a connection to an identity
            provider.
        """
        # TODO: This should take inputs
        await super().create_idp_connector()

    async def list_idp_connectors(self) -> List[IdpConnector]:
        """List IDP connectors in the identity server.

        Returns
        -------
        List[identity_proto.IdpConnector]
            A list of probotuf objects that return info on the connector ID,
            name, type, config version, and configuration of the upstream IDP
            connector.
        """
        response = await super().list_idp_connectors()
        return response.connectors

    async def update_idp_connector(self, connector: IdpConnector) -> None:
        """Update an IDP connector in the identity server.

        Parameters
        ----------
        connector : identity_proto.IdpConnector
            A protobuf object that represents a connection to an identity
            provider.
        """
        # TODO: This should take inputs
        await super().update_idp_connector()

    async def get_idp_connector(self, id: str) -> IdpConnector:
        """Get an IDP connector in the identity server.

        Parameters
        ----------
        id : str
            The connector ID.

        Returns
        -------
        identity_proto.IdpConnector
            A protobuf object that returns info on the connector ID, name,
            type, config version, and configuration of the upstream IDP
            connector.
        """
        # TODO: This should take inputs
        response = await super().get_idp_connector()
        return response.connector

    async def delete_idp_connector(self, id: str) -> None:
        """Delete an IDP connector in the identity server.

        Parameters
        ----------
        id : str
            The connector ID.
        """
        # TODO: This should take inputs
        await super().delete_idp_connector()

    async def create_oidc_client(self, client: OidcClient) -> OidcClient:
        """Create an OIDC client in the identity server.

        Parameters
        ----------
        client : identity_proto.OidcClient
            A protobuf object representing an OIDC client.

        Returns
        -------
        identity_proto.OidcClient
            A protobuf object that returns a client with info on the client ID,
            name, secret, and lists of redirect URIs and trusted peers.
        """
        # TODO: This should take inputs
        response = await super().create_oidc_client()
        return response.client

    async def update_oidc_client(self, client: OidcClient) -> None:
        """Update an OIDC client in the identity server.

        Parameters
        ----------
        client : identity_proto.OidcClient
            A protobuf object representing an OIDC client.

        """
        # TODO: This should take inputs
        await super().update_oidc_client()

    async def get_oidc_client(self, id: str) -> OidcClient:
        """Get an OIDC client in the identity server.

        Parameters
        ----------
        id : str
            The client ID.

        Returns
        -------
        identity_proto.OidcClient
            A protobuf object that returns a client with info on the client ID,
            name, secret, and lists of redirect URIs and trusted peers.
        """
        # TODO: This should take inputs
        response = await super().get_oidc_client()
        return response.client

    async def delete_oidc_client(self, id: str) -> None:
        """Delete an OIDC client in the identity server.

        Parameters
        ----------
        id : str
            The client ID.
        """
        # TODO: This should take inputs
        await super().delete_oidc_client()

    async def list_oidc_clients(self) -> List[OidcClient]:
        """List OIDC clients in the identity server.

        Returns
        -------
        List[identity_proto.OidcClient]
            A list of protobuf objects that return a client with info on the
            client ID, name, secret, and lists of redirect URIs and trusted
            peers.
        """
        response = await super().list_oidc_clients()
        return response.clients

    async def delete_all(self) -> None:
        """Delete all identity service information.

        Raises
        ------
        AuthServiceNotActivated
        """
        try:
            await super().delete_all()
        except grpc.RpcError as err:
            raise AuthServiceNotActivated.try_from(err)
