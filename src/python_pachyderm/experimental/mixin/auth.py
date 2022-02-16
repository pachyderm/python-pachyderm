from typing import Dict, List

import grpc

from python_pachyderm.errors import AuthServiceNotActivated
from ..proto.v2.auth_v2 import (
    ApiStub as _AuthApiStub,
    AuthorizeResponse,
    GetOidcLoginResponse,
    WhoAmIResponse,
    OidcConfig,
    Permission,
    Resource,
    ResourceType,
    Role,
    Roles,
)
from . import _synchronizer

# bp_to_pb: OidcConfig -> OIDCConfig
# bp_to_pb: GetOidcLoginResponse -> GetOIDCLoginResponse


@_synchronizer
class AuthApi(_synchronizer(_AuthApiStub)):
    """A mixin for auth-related functionality."""

    async def activate(self, root_token: str = None) -> str:
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
        response = await super().activate(root_token=root_token)
        return response.pach_token

    async def deactivate(self) -> None:
        """Deactivates auth, removing all ACLs, tokens, and admins from the
        Pachyderm cluster and making all data publicly accessible.

        Raises
        ------
        AuthServiceNotActivated
        """
        try:
            await super().deactivate()
        except grpc.RpcError as err:
            raise AuthServiceNotActivated.try_from(err)

    async def get_configuration(self) -> OidcConfig:
        """Gets the auth configuration.

        Returns
        -------
        auth_proto.OidcConfig
            A protobuf object with auth configuration information.
        """
        response = await super().get_configuration()
        return response.configuration

    async def set_configuration(self, configuration: OidcConfig) -> None:
        """Sets the auth configuration.

        Parameters
        ----------
        configuration : auth_proto.OidcConfig
            A protobuf object with auth configuration information.

        Examples
        --------
        >>> client.set_auth_configuration(OidcConfig(
        ...     issuer="http://localhost:1658",
        ...     client_id="client",
        ...     client_secret="secret",
        ...     redirect_uri="http://test.example.com",
        ... ))
        """
        await super().set_configuration(configuration=configuration)

    async def get_role_binding(self, resource: Resource) -> Dict[str, Roles]:
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
        >>> client.get_role_binding(Resource(type=ResourceType.CLUSTER))
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
        >>> client.get_role_binding(Resource(type=ResourceType.REPO, name="foobar"))
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
        response = await super().get_role_binding(resource=resource)
        return response.binding.entries

    async def modify_role_binding(
        self, resource: Resource, principal: str, roles: List[str] = None
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
        ...     Resource(type=ResourceType.CLUSTER),
        ...     "user:someuser",
        ...     roles=["clusterAdmin"]
        ... )
        >>> client.modify_role_binding(
        ...     Resource(type=ResourceType.REPO, name="foobar"),
        ...     "user:someuser",
        ...     roles=["repoWriter"]
        ... )
        """
        await super().modify_role_binding(
            resource=resource,
            principal=principal,
            roles=roles,
        )

    async def get_oidc_login(self) -> GetOidcLoginResponse:
        """Gets the OIDC login configuration.

        Returns
        -------
        auth_proto.GetOidcLoginResponse
            A protobuf object with the login configuration information.
        """
        return await super().get_oidc_login()

    async def authenticate_oidc(self, oidc_state: str) -> str:
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
        response = await super().authenticate(oidc_state=oidc_state)
        return response.pach_token

    async def authenticate_id_token(self, id_token: str) -> str:
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
        response = await super().authenticate(id_token=id_token)
        return response.pach_token

    async def authorize(
        self,
        resource: Resource,
        permissions: List[Permission] = None,
    ) -> AuthorizeResponse:
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
        ...     Resource(type=ResourceType.REPO, name="foobar"),
        ...     [Permission.REPO_READ]
        ... )
        authorized: true
        satisfied: REPO_READ
        principal: "pach:root"
        """
        return await super().authorize(resource=resource, permissions=permissions)

    async def who_am_i(self) -> WhoAmIResponse:
        """Returns info about the user tied to this `Client`.

        Returns
        -------
        auth_proto.WhoAmIResponse
            A protobuf object that returns the username and expiration for the
            token used.
        """
        return await super().who_am_i()

    async def get_roles_for_permission(self, permission: Permission) -> List[Role]:
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

        >>> roles = client.get_roles_for_permission(Permission.REPO_READ)

        .. # noqa: W505
        """
        response = await super().get_roles_for_permission(permission=permission)
        return response.roles

    async def get_robot_token(self, robot: str, ttl: int = None) -> str:
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
        response = await super().get_robot_token(robot=robot, ttl=ttl)
        return response.token

    async def revoke_auth_token(self, token: str) -> None:
        """Revokes an auth token.

        Parameters
        ----------
        token : str
            The Pachyderm token being revoked.
        """
        await super().revoke_auth_token(token=token)

    async def set_groups_for_user(self, username: str, groups: List[str]) -> None:
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
        await super().set_groups_for_user(username=username, groups=groups)

    async def modify_members(
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
        await super().modify_members(group=group, add=add, remove=remove)

    async def get_groups(self) -> List[str]:
        """Gets a list of groups this user belongs to.

        Returns
        -------
        List[str]
            List of groups the user belongs to.
        """
        response = await super().get_groups()
        return response.groups

    async def get_users(self, group: str) -> List[str]:
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
        response = await super().get_users(group=group)
        return response.usernames

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
