from python_pachyderm.service import Service


class EnterpriseMixin:
    def activate_enterprise(self, license_server, id, secret):
        """
        Activates enterprise by registering with a license server.
        Returns a `TokenInfo` object.

        Params:

        * `license_server`: The Pachyderm Enterprise Server to register with.
        * `id`: The unique ID for this cluster.
        * `secret`: The secret for registering this cluster.
        """
        self._req(
            Service.ENTERPRISE,
            "Activate",
            license_server=license_server,
            id=id,
            secret=secret,
        )

    def get_enterprise_state(self):
        """
        Gets the current enterprise state of the cluster. Returns a
        `GetEnterpriseResponse` object.
        """
        return self._req(Service.ENTERPRISE, "GetState")

    def deactivate_enterprise(self):
        """Deactivates enterprise."""
        self._req(Service.ENTERPRISE, "Deactivate")

    def get_activation_code(self):
        """
        Returns the enterprise code used to activate Pachdyerm Enterprise in
        this cluster.
        """
        return self._req(Service.ENTERPRISE, "GetActivationCode")
