from python_pachyderm.service import Service


class EnterpriseMixin:
    def activate_enterprise(self, activation_code, expires=None):
        """Activates enterprise. Returns a `TokenInfo` object.

        Parameters
        ----------
        activation_code : str
            Specifies a Pachyderm enterprise activation code. New users can
            obtain trial activation codes.
        expires : Timestamp protobuf, optional
            An optional ``Timestamp`` object indicating when this activation
            code will expire. This should not generally be set (it's primarily
            used for testing), and is only applied if it's earlier than the
            signed expiration time in `activation_code`.
        """
        return self._req(
            Service.ENTERPRISE,
            "Activate",
            activation_code=activation_code,
            expires=expires,
        ).info

    def get_enterprise_state(self):
        """Gets the current enterprise state of the cluster. Returns a
        ``GetEnterpriseResponse`` object.
        """
        return self._req(Service.ENTERPRISE, "GetState")

    def deactivate_enterprise(self):
        """Deactivates enterprise."""
        return self._req(Service.ENTERPRISE, "Deactivate")

    def get_activation_code(self):
        """Returns the enterprise code used to activate Pachdyerm Enterprise in
        this cluster.
        """
        return self._req(Service.ENTERPRISE, "GetActivationCode")
