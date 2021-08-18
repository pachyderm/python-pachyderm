from typing import List
from python_pachyderm.service import Service, identity_proto


class IdentityMixin:
    def set_identity_server_config(
        self, config: identity_proto.IdentityServerConfig
    ) -> None:
        """
        Configure the embedded identity server.

        Params:

        * `issuer`: The issuer for the identity server.
        """
        self._req(Service.IDENTITY, "SetIdentityServerConfig", config=config)

    def get_identity_server_config(self) -> identity_proto.IdentityServerConfig:
        """
        Get the embedded identity server configuration
        """
        return self._req(Service.IDENTITY, "GetIdentityServerConfig").config

    def create_idp_connector(self, connector: identity_proto.IDPConnector) -> None:
        """
        Create an IDP connector in the identity server.
        """
        self._req(Service.IDENTITY, "CreateIDPConnector", connector=connector)

    def list_idp_connectors(self) -> List[identity_proto.IDPConnector]:
        """
        List IDP connectors in the identity server.
        """
        return self._req(Service.IDENTITY, "ListIDPConnectors").connectors

    def update_idp_connector(self, connector: identity_proto.IDPConnector) -> None:
        """
        Update an IDP connector in the identity server.
        """
        self._req(Service.IDENTITY, "UpdateIDPConnector", connector=connector)

    def get_idp_connector(self, id: str) -> identity_proto.IDPConnector:
        """
        Get an IDP connector in the identity server.
        """
        return self._req(Service.IDENTITY, "GetIDPConnector", id=id).connector

    def delete_idp_connector(self, id: str) -> None:
        """
        Delete an IDP connector in the identity server.
        """
        self._req(Service.IDENTITY, "DeleteIDPConnector", id=id)

    def create_oidc_client(
        self, client: identity_proto.OIDCClient
    ) -> identity_proto.OIDCClient:
        """
        Create an OIDC client in the identity server.
        """
        return self._req(Service.IDENTITY, "CreateOIDCClient", client=client).client

    def update_oidc_client(self, client: identity_proto.OIDCClient):
        """
        Update an OIDC client in the identity server.
        """
        return self._req(Service.IDENTITY, "UpdateOIDCClient", client=client)

    def get_oidc_client(self, id: str) -> identity_proto.OIDCClient:
        """
        Get an OIDC client in the identity server.
        """
        return self._req(Service.IDENTITY, "GetOIDCClient", id=id).client

    def delete_oidc_client(self, id: str) -> None:
        """
        Delete an OIDC client in the identity server.
        """
        self._req(Service.IDENTITY, "DeleteOIDCClient", id=id)

    def list_oidc_clients(self) -> List[identity_proto.OIDCClient]:
        """
        List OIDC clients in the identity server.
        """
        return self._req(Service.IDENTITY, "ListOIDCClients").clients

    def delete_all_identity(self) -> None:
        """
        Delete all identity service information.
        """
        self._req(Service.IDENTITY, "DeleteAll")
