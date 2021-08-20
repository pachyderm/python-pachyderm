from python_pachyderm.service import Service


class LicenseMixin:
    def activate_license(self, activation_code, expires=None):
        """
        Activates the license service. Returns a `TokenInfo` object.

        Params:

        * `activation_code`: A string specifying a Pachyderm enterprise
        activation code. New users can obtain trial activation codes.
        * `expires`: An optional `Timestamp` object indicating when this
        activation code will expire. This should not generally be set (it's
        primarily used for testing), and is only applied if it's earlier than
        the signed expiration time in `activation_code`.
        """
        return self._req(
            Service.LICENSE,
            "Activate",
            activation_code=activation_code,
            expires=expires,
        )

    def add_cluster(
        self,
        id,
        address,
        secret=None,
        user_address=None,
        cluster_deployment_id=None,
        enterprise_server=None,
    ):
        """
        Register a cluster with the license service.

        Params:

        * `id`: A unique ID to identify the cluster.
        * `address`: A GRPC address for the license server to reach the cluster.
        * `secret`: Optional. A shared secret for the cluster to use
        to authenticate.
        If not specified, a random secret will be generated and returned.
        """
        return self._req(
            Service.LICENSE,
            "AddCluster",
            id=id,
            address=address,
            secret=secret,
            user_address=user_address,
            cluster_deployment_id=cluster_deployment_id,
            enterprise_server=enterprise_server,
        )

    def update_cluster(
        self, id, address, user_address=None, cluster_deployment_id=None
    ):
        """
        Update a cluster registered with the license service.

        Params:

        * `id`: The unique ID to identify the cluster.
        * `address`: A GRPC address for the license server to reach the cluster.
        """
        self._req(
            Service.LICENSE,
            "UpdateCluster",
            id=id,
            address=address,
            user_address=user_address,
            cluster_deployment_id=cluster_deployment_id,
        )

    def delete_cluster(self, id):
        """
        Delete a cluster registered with the license service.

        Params:

        * `id`: The unique ID to identify the cluster.
        """
        self._req(Service.LICENSE, "DeleteCluster", id=id)

    def list_clusters(self):
        """
        List clusters registered with the license service.
        """
        return self._req(Service.LICENSE, "ListClusters")

    def get_activation_code(self):
        """
        Returns the enterprise code used to activate the server.
        """
        return self._req(Service.LICENSE, "GetActivationCode")

    def delete_all_license(self):
        """
        Remove all clusters and deactivate the license service.
        """
        self._req(Service.LICENSE, "DeleteAll")

    def list_user_clusters(self):
        """
        Lists all clusters available to user.
        """
        return self._req(Service.LICENSE, "ListUserClusters")
