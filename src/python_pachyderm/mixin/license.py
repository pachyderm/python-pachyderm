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
        return self._req(Service.LICENSE, "Activate", activation_code=activation_code, expires=expires)

    def add_cluster(self, cluster_id, address, secret=None):
        """
        Register a cluster with the license service.

        Params:

        * `cluster_id`: A unique ID to identify the cluster.
        * `address`: A GRPC address for the license server to reach the cluster.
        * `secret`: Optional. A shared secret for the cluster to use to authenticate.
        If not specified, a random secret will be generated and returned.
        """
        return self._req(Service.LICENSE, "AddCluster", id=cluster_id, address=address, secret=secret)

    def update_cluster(self, cluster_id, address):
        """
        Update a cluster registered with the license service.

        Params:

        * `cluster_id`: The unique ID to identify the cluster.
        * `address`: A GRPC address for the license server to reach the cluster.
        """
        return self._req(Service.LICENSE, "UpdateCluster", id=cluster_id, address=address)

    def delete_cluster(self, cluster_id):
        """
        Delete a cluster registered with the license service.

        Params:

        * `cluster_id`: The unique ID to identify the cluster.
        """
        return self._req(Service.LICENSE, "DeleteCluster", id=cluster_id)

    def list_clusters(self):
        """
        List clusters registered with the license service.
        """
        return self._req(Service.LICENSE, "ListClusters")

    def delete_all_license(self):
        """
        Remove all clusters and deactivate the license service.
        """
        return self._req(Service.LICENSE, "DeleteAll")
