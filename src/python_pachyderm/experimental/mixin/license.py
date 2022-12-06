from typing import List
from grpc import RpcError

from python_pachyderm.errors import AuthServiceNotActivated
from python_pachyderm.service import Service
from python_pachyderm.experimental.service import license_proto, enterprise_proto
from google.protobuf import timestamp_pb2
import datetime

# bp_to_pb: datetime.datetime -> timestamp_pb2.Timestamp


class LicenseMixin:
    """A mixin for license-related functionality."""

    def activate_license(
        self, activation_code: str, expires: datetime.datetime = None
    ) -> enterprise_proto.TokenInfo:
        """Activates the license service.

        Parameters
        ----------
        activation_code : str
            A Pachyderm enterprise activation code. New users can obtain trial
            activation codes.
        expires : datetime.datetime, optional
            A protobuf object indicating when this activation code will expire.
            This should generally not be set and is only applied if it is
            earlier than the signed expiration time of `activation_code`.

        Returns
        -------
        enterprise_proto.TokenInfo
            A protobuf object that has the expiration for the current token.
            Field will be unset if there is no current token.
        """
        return self._req(
            Service.LICENSE,
            "Activate",
            activation_code=activation_code,
            expires=expires,
        ).info

    def add_cluster(
        self,
        id: str,
        address: str,
        secret: str = None,
        user_address: str = None,
        cluster_deployment_id: str = None,
        enterprise_server: bool = False,
    ) -> license_proto.AddClusterResponse:
        """Register a cluster with the license service.

        Parameters
        ----------
        id : str
            The unique ID to identify the cluster.
        address : str
            The public GRPC address for the license server to reach the cluster.
        secret : str, optional
            A shared secret for the cluster to use to authenticate. If not
            specified, a random secret will be generated and returned.
        user_address : str, optional
            The pachd address for users to connect to.
        cluster_deployment_id : str, optional
            The deployment ID value generated by each cluster.
        enterprise_server : bool, optional
            Indicates whether the address points to an enterprise server.

        Returns
        -------
        license_proto.AddClusterResponse
            A protobuf object that returns the `secret`.
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
        self,
        id: str,
        address: str,
        user_address: str = None,
        cluster_deployment_id: str = None,
        secret: str = None,
    ) -> None:
        """Update a cluster registered with the license service.

        Parameters
        ----------
        id : str
            The unique ID to identify the cluster.
        address : str
            The public GRPC address for the license server to reach the cluster.
        user_address : str, optional
            The pachd address for users to connect to.
        cluster_deployment_id : str, optional
            The deployment ID value generated by each cluster.
        secret : str, optional
            A shared secret for the cluster to use to authenticate. If not
            specified, a random secret will be generated and returned.
        """
        self._req(
            Service.LICENSE,
            "UpdateCluster",
            id=id,
            address=address,
            user_address=user_address,
            cluster_deployment_id=cluster_deployment_id,
            secret=secret,
        )

    def delete_cluster(self, id: str) -> None:
        """Delete a cluster registered with the license service.

        Parameters
        ----------
        id : str
            The unique ID to identify the cluster.
        """
        self._req(Service.LICENSE, "DeleteCluster", id=id)

    def list_clusters(self) -> List[license_proto.ClusterStatus]:
        """List clusters registered with the license service.

        Returns
        -------
        List[license_proto.ClusterStatus]
            A list of protobuf objects that return info on a cluster.
        """
        return self._req(Service.LICENSE, "ListClusters").clusters

    def get_activation_code(self) -> license_proto.GetActivationCodeResponse:
        """Gets the enterprise code used to activate the server.

        Returns
        -------
        license_proto.GetActivationCodeResponse
            A protobuf object that returns a state enum, info on the token,
            and the activation code.
        """
        return self._req(Service.LICENSE, "GetActivationCode")

    def delete_all_license(self) -> None:
        """Remove all clusters and deactivate the license service.

        Raises
        ------
        AuthServiceNotActivated
        """
        try:
            self._req(Service.LICENSE, "DeleteAll")
        except RpcError as err:
            raise AuthServiceNotActivated.try_from(err)

    def list_user_clusters(self) -> List[license_proto.UserClusterInfo]:
        """Lists all clusters available to user.

        Returns
        -------
        List[license_proto.UserClusterInfo]
            A list of protobuf objects that return info on clusters
            available to the users.
        """
        return self._req(Service.LICENSE, "ListUserClusters").clusters
