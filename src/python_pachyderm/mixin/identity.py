from python_pachyderm.service import Service


class IdentityMixin:
    def set_identity_server_config(self, config):
        """
        Configure the embedded identity server.

        Params:

        * `issuer`: The issuer for the identity server.
        """
        self._req(Service.IDENTITY, "SetIdentityServerConfig", config=config)

    def get_identity_server_config(self):
        """
        Get the embedded identity server configuration
        """
        return self._req(Service.IDENTITY, "GetIdentityServerConfig")

    def create_idp_connector(self, connector):
        """
        Create an IDP connector in the identity server.
        """
        self._req(Service.IDENTITY, "CreateIDPConnector", connector=connector)

    def list_idp_connectors(self):
        """
        List IDP connectors in the identity server.
        """
        return self._req(Service.IDENTITY, "ListIDPConnectors")

    def update_idp_connector(self, connector):
        """
        Update an IDP connector in the identity server.
        """
        self._req(Service.IDENTITY, "UpdateIDPConnector", connector=connector)

    def get_idp_connector(self, id):
        """
        Get an IDP connector in the identity server.
        """
        return self._req(Service.IDENTITY, "GetIDPConnector", id=id)

    def delete_idp_connector(self, id):
        """
        Delete an IDP connector in the identity server.
        """
        self._req(Service.IDENTITY, "DeleteIDPConnector", id=id)

    def create_oidc_client(self, client):
        """
        Create an OIDC client in the identity server.
        """
        return self._req(Service.IDENTITY, "CreateOIDCClient", client=client)

    def update_oidc_client(self, client):
        """
        Update an OIDC client in the identity server.
        """
        return self._req(Service.IDENTITY, "UpdateOIDCClient", client=client)

    def get_oidc_client(self, id):
        """
        Get an OIDC client in the identity server.
        """
        return self._req(Service.IDENTITY, "GetOIDCClient", id=id)

    def delete_oidc_client(self, id):
        """
        Delete an OIDC client in the identity server.
        """
        self._req(Service.IDENTITY, "DeleteOIDCClient", id=id)

    def list_oidc_clients(self):
        """
        List OIDC clients in the identity server.
        """
        return self._req(Service.IDENTITY, "ListOIDCClients")

    def delete_all_identity(self):
        """
        Delete all identity service information.
        """
        self._req(Service.IDENTITY, "DeleteAll")
