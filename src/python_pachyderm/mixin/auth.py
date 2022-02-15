from typing import Dict, List

import grpc

from python_pachyderm.errors import AuthServiceNotActivated
from python_pachyderm.proto.v2.auth import auth_pb2, auth_pb2_grpc


class AuthMixin:
    """A mixin for auth-related functionality."""

    _channel: grpc.Channel

    def __init__(self):
        self.__stub = auth_pb2_grpc.APIStub(self._channel)
        super().__init__()

    def activate_auth(self, root_token: str = None) -> str:
        """Activates auth on the cluster. Returns the root token, an
        irrevocable superuser credential that should be stored securely.

        Parameters
        ----------
        root_token : str, optional
            If set, the token used as the root user login token. In general,
            it is safer to not set and let Pachyderm generate one for you.

        Returns
        -------
        str
            A token used as the root user login token.
        """
        message = auth_pb2.ActivateRequest(root_token=root_token)
        return self.__stub.Activate(message).pach_token

    def deactivate_auth(self) -> None:
        """Deactivates auth, removing all ACLs, tokens, and admins from the
        Pachyderm cluster and making all data publicly accessible.

        Raises
        ------
        AuthServiceNotActivated
        """
        message = auth_pb2.DeactivateRequest()
        try:
            self.__stub.Deactivate(message)
        except grpc.RpcError as err:
            raise AuthServiceNotActivated.try_from(err)

    def get_auth_configuration(self) -> auth_pb2.OIDCConfig:
        """Gets the auth configuration.

        Returns
        -------
        auth_pb2.OIDCConfig
            A protobuf object with auth configuration information.
        """
        message = auth_pb2.GetConfigurationRequest()
        return self.__stub.GetConfiguration(message).configuration

    def set_auth_configuration(self, configuration: auth_pb2.OIDCConfig) -> None:
        """Sets the auth configuration.

        Parameters
        ----------
        configuration : auth_pb2.OIDCConfig
            A protobuf object with auth configuration information.

        Examples
        --------
        >>> client.set_auth_configuration(auth_pb2.OIDCConfig(
        ...     issuer="http://localhost:1658",
        ...     client_id="client",
        ...     client_secret="secret",
        ...     redirect_uri="http://test.example.com",
        ... ))
        """
        message = auth_pb2.SetConfigurationRequest(configuration=configuration)
        self.__stub.SetConfiguration(message)

    def get_role_binding(
        self, resource: auth_pb2.Resource
    ) -> Dict[str, auth_pb2.Roles]:
        """Returns the current set of role bindings to the resource specified.

        Parameters
        ----------
        resource : auth_pb2.Resource
            A protobuf object representing the resource being checked.

        Returns
        -------
        Dict[str, auth_pb2.Roles]
            A dictionary mapping the principals to the roles they have.

        Examples
        --------
        >>> client.get_role_binding(auth_pb2.Resource(type=auth_pb2.CLUSTER))
        {
            'robot:someuser': roles {
                key: "clusterAdmin"
                value: true
            },
            'pach:root': roles {
                key: "clusterAdmin"
                value: true
            }
        }
        ...
        >>> client.get_role_binding(auth_pb2.Resource(type=auth_pb2.REPO, name="foobar"))
        {
            'user:person_a': roles {
                key: "repoWriter"
                value: true
            },
            'pach:root': roles {
                key: "repoOwner"
                value: true
            }
        }

        .. # noqa: W505
        """
        message = auth_pb2.GetRoleBindingRequest(resource=resource)
        return self.__stub.GetRoleBinding(message).binding.entries

    def modify_role_binding(
        self, resource: auth_pb2.Resource, principal: str, roles: List[str] = None
    ) -> None:
        """Sets the roles for a given principal on a resource.

        Parameters
        ----------
        resource : auth_pb2.Resource
            A protobuf object representing the resource to grant the roles on.
        principal : str
            The principal to grant the roles for.
        roles : List[str], optional
            The absolute list of roles for the principtal to have. If roles is
            unset, the principal will have no roles.

        Examples
        --------
        You can find some of the built-in roles here:
        https://github.com/pachyderm/pachyderm/blob/master/src/auth/auth.go#L27

        >>> client.modify_role_binding(
        ...     auth_pb2.Resource(type=auth_pb2.CLUSTER),
        ...     "user:someuser",
        ...     roles=["clusterAdmin"]
        ... )
        >>> client.modify_role_binding(
        ...     auth_pb2.Resource(type=auth_pb2.REPO, name="foobar"),
        ...     "user:someuser",
        ...     roles=["repoWriter"]
        ... )
        """
        message = auth_pb2.ModifyRoleBindingRequest(
            resource=resource,
            principal=principal,
            roles=roles,
        )
        self.__stub.ModifyRoleBinding(message)

    def get_oidc_login(self) -> auth_pb2.GetOIDCLoginResponse:
        """Gets the OIDC login configuration.

        Returns
        -------
        auth_pb2.GetOIDCLoginResponse
            A protobuf object with the login configuration information.
        """
        message = auth_pb2.GetOIDCLoginRequest()
        return self.__stub.GetOIDCLogin(message)

    def authenticate_oidc(self, oidc_state: str) -> str:
        """Authenticates a user to the Pachyderm cluster via OIDC.

        Parameters
        ----------
        oidc_state : str
            An OIDC state token.

        Returns
        -------
        str
            A token that can be used for making authenticate requests.
        """
        message = auth_pb2.AuthenticateRequest(oidc_state=oidc_state)
        return self.__stub.Authenticate(message).pach_token

    def authenticate_id_token(self, id_token: str) -> str:
        """Authenticates a user to the Pachyderm cluster using an ID token
        issued by the OIDC provider. The token must include the Pachyderm
        client_id in the set of audiences to be valid.

        Parameters
        ----------
        id_token : str
            The ID token.

        Returns
        -------
        str
            A token that can be used for making authenticate requests.
        """
        message = auth_pb2.AuthenticateRequest(id_token=id_token)
        return self.__stub.Authenticate(message).pach_token

    def authorize(
        self,
        resource: auth_pb2.Resource,
        permissions: List["auth_pb2.Permission"] = None,
    ) -> auth_pb2.AuthorizeResponse:
        """Tests a list of permissions that the user might have on a resource.

        Parameters
        ----------
        resource : auth_pb2.Resource
            The resource the user wants to test on.
        permissions : List[auth_pb2.Permission], optional
            The list of permissions the users wants to test.

        Returns
        -------
        auth_pb2.AuthorizeResponse
            A protobuf object that indicates whether the user/principal had all
            the inputted permissions on the resource, which permissions the
            user had, which permissions the user lacked, and the name of the
            principal.

        Examples
        --------
        >>> client.authorize(
        ...     auth_pb2.Resource(type=auth_pb2.REPO, name="foobar"),
        ...     [auth_pb2.Permission.REPO_READ]
        ... )
        authorized: true
        satisfied: REPO_READ
        principal: "pach:root"
        """
        message = auth_pb2.AuthorizeRequest(resource=resource, permissions=permissions)
        return self.__stub.Authorize(message)

    def who_am_i(self) -> auth_pb2.WhoAmIResponse:
        """Returns info about the user tied to this `Client`.

        Returns
        -------
        auth_pb2.WhoAmIResponse
            A protobuf object that returns the username and expiration for the
            token used.
        """
        message = auth_pb2.WhoAmIRequest()
        return self.__stub.WhoAmI(message)

    def get_roles_for_permission(
        self, permission: auth_pb2.Permission
    ) -> List[auth_pb2.Role]:
        """Returns a list of all roles that have the specified permission.

        Parameters
        ----------
        permission : auth_pb2.Permission
            The Permission enum to check for.

        Returns
        -------
        List[auth_pb2.Role]
            A list of Role protobuf objects that all have the specified
            permission.

        Examples
        --------
        All available permissions can be found in auth proto Permissions enum

        >>> roles = client.get_roles_for_permission(auth_pb2.Permission.REPO_READ)

        .. # noqa: W505
        """
        message = auth_pb2.GetRolesForPermissionRequest(permission=permission)
        return self.__stub.GetRolesForPermission(message).roles

    def get_robot_token(self, robot: str, ttl: int = None) -> str:
        """Gets a new auth token for a robot user.

        Parameters
        ----------
        robot : str
            The name of the robot user.
        ttl : int, optional
            The remaining lifetime of this token in seconds. If unset,
            token doesn't expire.

        Returns
        -------
        str
            The new auth token.
        """
        message = auth_pb2.GetRobotTokenRequest(robot=robot, ttl=ttl)
        return self.__stub.GetRobotToken(message).token

    def revoke_auth_token(self, token: str) -> None:
        """Revokes an auth token.

        Parameters
        ----------
        token : str
            The Pachyderm token being revoked.
        """
        message = auth_pb2.RevokeAuthTokenRequest(token=token)
        self.__stub.RevokeAuthToken(message)

    def set_groups_for_user(self, username: str, groups: List[str]) -> None:
        """Sets the group membership for a user.

        Parameters
        ----------
        username : str
            The username to be added.
        groups : List[str]
            The groups to add the username to.

        Examples
        --------
        >>> client.set_groups_for_user("user:someuser", ["foogroup", "bargroup"])

        .. # noqa: W505
        """
        message = auth_pb2.SetGroupsForUserRequest(username=username, groups=groups)
        self.__stub.SetGroupsForUser(message)

    def modify_members(
        self, group: str, add: List[str] = None, remove: List[str] = None
    ) -> None:
        """Adds and/or removes members of a group.

        Parameters
        ----------
        group : str
            The group to modify.
        add : List[str], optional
            A list of strings specifying members to add.
        remove : List[str], optional
            A list of strings specifying members to remove.

        Examples
        --------
        >>> client.modify_members(
        ...     "foogroup",
        ...     add=["user:otheruser"],
        ...     remove=["user:someuser"]
        ... )
        """
        message = auth_pb2.ModifyMembersRequest(group=group, add=add, remove=remove)
        self.__stub.ModifyMembers(message)

    def get_groups(self) -> List[str]:
        """Gets a list of groups this user belongs to.

        Returns
        -------
        List[str]
            List of groups the user belongs to.
        """
        message = auth_pb2.GetGroupsRequest()
        return self.__stub.GetGroups(message).groups

    def get_users(self, group: str) -> List[str]:
        """Gets users in a group.

        Parameters
        ----------
        group : str
            The group to list users from.

        Returns
        -------
        List[str]
            All the users in the specified group.
        """
        message = auth_pb2.GetUsersRequest(group=group)
        return self.__stub.GetUsers(message).usernames

    def extract_auth_tokens(self):
        """This maps to an internal function that is only used for migration.
        Pachyderm's `extract` and `restore` functionality calls
        `extract_auth_tokens` and `restore_auth_tokens` to move Pachyderm tokens
        between clusters during migration. Currently this function is only used
        for Pachyderm internals, so we're avoiding support for this function in
        python-pachyderm client until we find a use for it (feel free to file an
        issue in github.com/pachyderm/pachyderm).
        """
        raise NotImplementedError(
            "extract/restore are for testing and internal use only"
        )

    def restore_auth_token(self, token=None):
        """This maps to an internal function that is only used for migration.
        Pachyderm's `extract` and `restore` functionality calls
        `extract_auth_tokens` and `restore_auth_tokens` to move Pachyderm tokens
        between clusters during migration. Currently this function is only used
        for Pachyderm internals, so we're avoiding support for this function in
        python-pachyderm client until we find a use for it (feel free to file an
        issue in github.com/pachyderm/pachyderm).
        """
        raise NotImplementedError(
            "extract/restore are for testing and internal use only"
        )
