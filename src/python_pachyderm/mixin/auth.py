from python_pachyderm.service import Service


class AuthMixin:
    def activate_auth(self, root_token=None):
        """
        Activates auth. Returns the root token for the cluster, an
        irrevocable superuser credential that should be stored securely.

        Params:

        * `root_token`: An optional string. If specified, this string becomes
          the Pachyderm cluster root user's token (otherwise, Pachyderm
          generates a root token, which is generally safer). Currently this is
          only used for testing and Pachyderm internals (migration).
        """
        return self._req(Service.AUTH, "Activate", root_token=root_token).pach_token

    def deactivate_auth(self):
        """
        Deactivates auth, removing all ACLs, tokens, and admins from the
        Pachyderm cluster and making all data publicly accessible.
        """
        self._req(Service.AUTH, "Deactivate")

    def get_auth_configuration(self):
        """
        Gets the auth configuration. Returns an `AuthConfig` object.
        """
        return self._req(Service.AUTH, "GetConfiguration").configuration

    def set_auth_configuration(self, configuration):
        """
        Set the auth configuration.

        Params:

        * `config`: An `AuthConfig` object.
        """
        self._req(Service.AUTH, "SetConfiguration", configuration=configuration)

    def get_role_binding(self, resource):
        """
        Returns the current set of cluster role bindings.
        """
        return self._req(Service.AUTH, "GetRoleBinding", resource=resource)

    def modify_role_binding(self, resource, principal, roles=None):
        """
        Sets the roles for a given principal on a resource.

        Params:

        * `resource`: The resource to grant the roles on.
        * `principal`: A string specifying the principal.
        * `roles`: A list of roles to grant. If empty, all roles are revoked.
        """
        self._req(
            Service.AUTH,
            "ModifyRoleBinding",
            resource=resource,
            principal=principal,
            roles=roles,
        )

    def get_oidc_login(self):
        """
        Returns the OIDC login configuration.
        """
        return self._req(Service.AUTH, "GetOIDCLogin")

    def authenticate_oidc(self, oidc_state):
        """
        Authenticates a user to the Pachyderm cluster via OIDC. Returns a
        string that can be used for making authenticated requests.

        Params:

        * `oidc_state`: A string of the OIDC state token.
        """
        return self._req(Service.AUTH, "Authenticate", oidc_state=oidc_state).pach_token

    def authenticate_id_token(self, id_token):
        """
        Authenticates a user to the Pachyderm cluster using an ID token issued
        by the OIDC provider. The token must include the Pachyderm client_id
        in the set of audiences to be valid. Returns a string that can be used
        for making authenticated requests.

        Params:

        * `id_token`: A string of the ID token.
        """
        return self._req(Service.AUTH, "Authenticate", id_token=id_token).pach_token

    def authorize(self, resource, permissions=None):
        """
        Authorizes the user to a given resource.

        Params:

        * `resource`: The resource the user wants access to
        * `permissions`: A list of permissions the user wants to test.
        """
        return self._req(
            Service.AUTH, "Authorize", resource=resource, permissions=permissions
        )

    def who_am_i(self):
        """
        Returns info about the user tied to this `Client`.
        """
        return self._req(Service.AUTH, "WhoAmI")

    def get_roles_for_permission(self, permission):
        """
        Returns roles that have the specified permission.

        Params:

        * `permission`: The Permission enum to check for.
        """
        return self._req(Service.AUTH, "GetRolesForPermission", permission=permission)

    def get_robot_token(self, robot, ttl=None):
        """
        Gets a new auth token for a robot user.

        Params:

        * `robot`: The name of the robot user.
        * `ttl`: Optional. The expiration for the token.
        If empty, the token does not expire.
        """
        return self._req(Service.AUTH, "GetRobotToken", robot=robot, ttl=ttl).token

    def revoke_auth_token(self, token):
        """
        Revokes an auth token.

        Params:

        * `token`: A string that indicates the Pachyderm token that is being
        revoked.
        """
        self._req(Service.AUTH, "RevokeAuthToken", token=token)

    def set_groups_for_user(self, username, groups):
        """
        Sets the group membership for a user.

        Params:

        * `username`: A string.
        * `groups`: A list of strings.
        """
        self._req(Service.AUTH, "SetGroupsForUser", username=username, groups=groups)

    def modify_members(self, group, add=None, remove=None):
        """
        Adds and/or removes members of a group.

        Params:

        * `group`: A string.
        * `add`: An optional list of strings specifying members to add.
        * `remove`: An optional list of strings specifying members to remove.
        """
        self._req(
            Service.AUTH,
            "ModifyMembers",
            group=group,
            add=add or [],
            remove=remove or [],
        )

    def get_groups(self):
        """
        Gets a list of groups this user belongs to.
        """
        return self._req(Service.AUTH, "GetGroups").groups

    def get_users(self, group):
        """
        Gets which users below to the given `group`. Returns a list of strings.

        Params:

        `group`: A string.
        """
        return self._req(Service.AUTH, "GetUsers", group=group).usernames

    def extract_auth_tokens(self):
        """
        This maps to an internal function that is only used for migration.
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
        """
        This maps to an internal function that is only used for migration.
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
