from typing import Dict, List
from grpc import RpcError

from python_pachyderm.errors import AuthServiceNotActivated
from python_pachyderm.service import Service
from python_pachyderm.experimental.service import auth_proto

# bp_to_pb: OidcConfig -> OIDCConfig
# bp_to_pb: GetOidcLoginResponse -> GetOIDCLoginResponse


class AuthMixin:
    """A mixin for auth-related functionality."""

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
        return self._req(Service.AUTH, "Activate", root_token=root_token).pach_token

    def deactivate_auth(self) -> None:
        """Deactivates auth, removing all ACLs, tokens, and admins from the
        Pachyderm cluster and making all data publicly accessible.

        Raises
        ------
        AuthServiceNotActivated
        """
        try:
            self._req(Service.AUTH, "Deactivate")
        except RpcError as err:
            raise AuthServiceNotActivated.try_from(err)

    def get_auth_configuration(self) -> auth_proto.OidcConfig:
        """Gets the auth configuration.

        Returns
        -------
        auth_proto.OidcConfig
            A protobuf object with auth configuration information.
        """
        return self._req(Service.AUTH, "GetConfiguration").configuration

    def set_auth_configuration(self, configuration: auth_proto.OidcConfig) -> None:
        """Sets the auth configuration.

        Parameters
        ----------
        configuration : auth_proto.OidcConfig
            A protobuf object with auth configuration information.

        Examples
        --------
        >>> client.set_auth_configuration(auth_proto.OidcConfig(
        ...     issuer="http://localhost:1658",
        ...     client_id="client",
        ...     client_secret="secret",
        ...     redirect_uri="http://test.example.com",
        ... ))
        """
        self._req(Service.AUTH, "SetConfiguration", configuration=configuration)

    def get_role_binding(
        self, resource: auth_proto.Resource
    ) -> Dict[str, auth_proto.Roles]:
        """Returns the current set of role bindings to the resource specified.

        Parameters
        ----------
        resource : auth_proto.Resource
            A protobuf object representing the resource being checked.

        Returns
        -------
        Dict[str, auth_proto.Roles]
            A dictionary mapping the principals to the roles they have.

        Examples
        --------
        >>> client.get_role_binding(auth_proto.Resource(type=auth_proto.CLUSTER))
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
        >>> client.get_role_binding(auth_proto.Resource(type=auth_proto.REPO, name="foobar"))
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
        return self._req(
            Service.AUTH, "GetRoleBinding", resource=resource
        ).binding.entries

    def modify_role_binding(
        self, resource: auth_proto.Resource, principal: str, roles: List[str] = None
    ) -> None:
        """Sets the roles for a given principal on a resource.

        Parameters
        ----------
        resource : auth_proto.Resource
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
        ...     auth_proto.Resource(type=auth_proto.CLUSTER),
        ...     "user:someuser",
        ...     roles=["clusterAdmin"]
        ... )
        >>> client.modify_role_binding(
        ...     auth_proto.Resource(type=auth_proto.REPO, name="foobar"),
        ...     "user:someuser",
        ...     roles=["repoWriter"]
        ... )
        """
        self._req(
            Service.AUTH,
            "ModifyRoleBinding",
            resource=resource,
            principal=principal,
            roles=roles,
        )

    def get_oidc_login(self) -> auth_proto.GetOidcLoginResponse:
        """Gets the OIDC login configuration.

        Returns
        -------
        auth_proto.GetOidcLoginResponse
            A protobuf object with the login configuration information.
        """
        return self._req(Service.AUTH, "GetOIDCLogin")

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
        return self._req(Service.AUTH, "Authenticate", oidc_state=oidc_state).pach_token

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
        return self._req(Service.AUTH, "Authenticate", id_token=id_token).pach_token

    def authorize(
        self,
        resource: auth_proto.Resource,
        permissions: List["auth_proto.Permission"] = None,
    ) -> auth_proto.AuthorizeResponse:
        """Tests a list of permissions that the user might have on a resource.

        Parameters
        ----------
        resource : auth_proto.Resource
            The resource the user wants to test on.
        permissions : List[auth_proto.Permission], optional
            The list of permissions the users wants to test.

        Returns
        -------
        auth_proto.AuthorizeResponse
            A protobuf object that indicates whether the user/principal had all
            the inputted permissions on the resource, which permissions the
            user had, which permissions the user lacked, and the name of the
            principal.

        Examples
        --------
        >>> client.authorize(
        ...     auth_proto.Resource(type=auth_proto.REPO, name="foobar"),
        ...     [auth_proto.Permission.REPO_READ]
        ... )
        authorized: true
        satisfied: REPO_READ
        principal: "pach:root"
        """
        return self._req(
            Service.AUTH, "Authorize", resource=resource, permissions=permissions
        )

    def who_am_i(self) -> auth_proto.WhoAmIResponse:
        """Returns info about the user tied to this `Client`.

        Returns
        -------
        auth_proto.WhoAmIResponse
            A protobuf object that returns the username and expiration for the
            token used.
        """
        return self._req(Service.AUTH, "WhoAmI")

    def get_roles_for_permission(
        self, permission: auth_proto.Permission
    ) -> List[auth_proto.Role]:
        """Returns a list of all roles that have the specified permission.

        Parameters
        ----------
        permission : auth_proto.Permission
            The Permission enum to check for.

        Returns
        -------
        List[auth_proto.Role]
            A list of Role protobuf objects that all have the specified
            permission.

        Examples
        --------
        All available permissions can be found in auth proto Permissions enum

        >>> roles = client.get_roles_for_permission(auth_proto.Permission.REPO_READ)

        .. # noqa: W505
        """
        return self._req(
            Service.AUTH, "GetRolesForPermission", permission=permission
        ).roles

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
        return self._req(Service.AUTH, "GetRobotToken", robot=robot, ttl=ttl).token

    def revoke_auth_token(self, token: str) -> None:
        """Revokes an auth token.

        Parameters
        ----------
        token : str
            The Pachyderm token being revoked.
        """
        self._req(Service.AUTH, "RevokeAuthToken", token=token)

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
        self._req(Service.AUTH, "SetGroupsForUser", username=username, groups=groups)

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
        self._req(
            Service.AUTH,
            "ModifyMembers",
            group=group,
            add=add,
            remove=remove,
        )

    def get_groups(self) -> List[str]:
        """Gets a list of groups this user belongs to.

        Returns
        -------
        List[str]
            List of groups the user belongs to.
        """
        return self._req(Service.AUTH, "GetGroups").groups

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
        return self._req(Service.AUTH, "GetUsers", group=group).usernames

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
