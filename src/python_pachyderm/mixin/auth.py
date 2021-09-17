import warnings
from python_pachyderm.service import Service


class AuthMixin:
    def activate_auth(self, subject, github_token=None, root_token=None):
        """Activates auth, creating an initial set of admins. Returns a string
        that can be used for making authenticated requests.

        Parameters
        ----------
        subject : str
            If set to a github user (i.e. it has a 'github:' prefix or no
            prefix) then Pachyderm will confirm that it matches the user
            associated with `github_token`.
            If set to a robot user (i.e. it has a 'robot:' prefix), then
            Pachyderm will generate a new token for the robot user; this token
            will be the only way to administer this cluster until more admins
            are added.
        github_token : str, optional
            This is the token returned by GitHub and used to authenticate the
            caller. When Pachyderm is deployed locally, setting this value to a
            given string will automatically authenticate the caller as a GitHub
            user whose username is that string (unless this "looks like" a
            GitHub access code, in which case Pachyderm does retrieve the
            corresponding GitHub username)
        root_token : str, optional
            Unused
        """
        return self._req(
            Service.AUTH, "Activate", subject=subject, github_token=github_token
        ).pach_token

    def deactivate_auth(self):
        """Deactivates auth, removing all ACLs, tokens, and admins from the
        Pachyderm cluster and making all data publicly accessible.
        """
        return self._req(Service.AUTH, "Deactivate")

    def get_auth_configuration(self):
        """
        Gets the auth configuration. Returns an `AuthConfig` object.
        """
        return self._req(Service.AUTH, "GetConfiguration").configuration

    def set_auth_configuration(self, configuration):
        """Set the auth configuration.

        Parameters
        ----------
        config : AuthConfig protobuf
            The auth configuration.
        """
        return self._req(Service.AUTH, "SetConfiguration", configuration=configuration)

    def get_admins(self):
        """Returns a list of strings specifying the cluster admins."""
        warnings.warn(
            "deprecated in 1.11, use 'get_cluster_role_bindings' instead",
            DeprecationWarning,
        )
        return self._req(Service.AUTH, "GetAdmins").admins

    def modify_admins(self, add=None, remove=None):
        """Adds and/or removes admins.

        Parameters
        ----------
        add : List[str], optional
            A list of strings specifying admins to add.
        remove : List[str], optional
            A list of strings specifying admins to remove.
        """
        warnings.warn(
            "deprecated in 1.11, use 'modify_cluster_role_binding' instead",
            DeprecationWarning,
        )
        return self._req(
            Service.AUTH, "ModifyAdmins", add=add or [], remove=remove or []
        )

    def get_cluster_role_bindings(self):
        """Returns the current set of cluster role bindings."""
        return self._req(Service.AUTH, "GetClusterRoleBindings")

    def modify_cluster_role_binding(self, principal, roles=None):
        """Sets the list of admin roles for a principal.

        Parameters
        ----------
        principal : str, optional
            A string specifying the principal.
        roles : ClusterRoles protobuf
            A `ClusterRoles` object specifying cluster-wide permissions the
            principal has. If unspecified, all roles are revoked for the
            principal.
        """
        return self._req(
            Service.AUTH, "ModifyClusterRoleBinding", principal=principal, roles=roles
        )

    def get_oidc_login(self):
        """Returns the OIDC login configuration."""
        return self._req(Service.AUTH, "GetOIDCLogin")

    def authenticate_github(self, github_token):
        """Authenticates a GitHub user to the Pachyderm cluster. Returns a
        string that can be used for making authenticated requests.

        Parameters
        ----------
        github_token: str
            This is the token returned by GitHub and used to authenticate the
            caller. When Pachyderm is deployed locally, setting this value to
            a given string will automatically authenticate the caller as a
            GitHub user whose username is that string (unless this "looks like"
            a GitHub access code, in which case Pachyderm does retrieve the
            corresponding GitHub username.)
        """
        return self._req(
            Service.AUTH, "Authenticate", github_token=github_token
        ).pach_token

    def authenticate_oidc(self, oidc_state):
        """Authenticates a user to the Pachyderm cluster via OIDC. Returns a
        string that can be used for making authenticated requests.

        Parameters
        ----------
        oidc_state : str
            The OIDC state token.
        """
        return self._req(Service.AUTH, "Authenticate", oidc_state=oidc_state).pach_token

    def authenticate_id_token(self, id_token):
        """Authenticates a user to the Pachyderm cluster using an ID token
        issued by the OIDC provider. The token must include the Pachyderm
        client_id in the set of audiences to be valid. Returns a string that
        can be used for making authenticated requests.

        Parameters
        ----------
        id_token : str
            The ID token.
        """
        return self._req(Service.AUTH, "Authenticate", id_token=id_token).pach_token

    def authenticate_one_time_password(self, one_time_password):
        """Authenticates a user to the Pachyderm cluster using a one-time
        password. Returns a string that can be used for making authenticated
        requests.

        Parameters
        ----------
        one_time_password : str
            This is a short-lived, one-time-use password generated by
            Pachyderm, for the purpose of propagating authentication to new
            clients (e.g. from the dash to pachd.)
        """
        return self._req(
            Service.AUTH, "Authenticate", one_time_password=one_time_password
        ).pach_token

    def authorize(self, repo, scope):
        """Authorizes the user to a given repo/scope. Return a bool specifying
        if the caller has at least `scope`-level access to `repo`.

        Parameters
        ----------
        repo : str
            The repo name that the caller wants access to.
        scope : int
            The access level that the caller needs to perform an action. See
            the ``Scope`` enum for variants.
        """
        return self._req(Service.AUTH, "Authorize", repo=repo, scope=scope).authorized

    def who_am_i(self):
        """Returns info about the user tied to this :class:`.Client`."""
        return self._req(Service.AUTH, "WhoAmI")

    def get_scope(self, username, repos):
        """Gets the auth scope. Returns a list of `Scope` objects.

        Parameters
        ----------
        username : str
            A string specifying the principal (some of which belong to robots
            rather than users, but the name is preserved for now to provide
            compatibility with the pachyderm dash) whose access level is
            queried. To query the access level of a robot user, the caller must
            prefix username with "robot:". If `username` has no prefix (i.e.
            no ":"), then it's assumed to be a github user's principal.
        repos : List[str]
            A list of strings specifying the objects to which `username`s
            access level is being queried
        """
        return self._req(
            Service.AUTH, "GetScope", username=username, repos=repos
        ).scopes

    def set_scope(self, username, repo, scope):
        """Set the auth scope.

        Parameters
        ----------
        username : str
            A string specifying the principal (some of which belong to robots
            rather than users, but the name is preserved for now to provide
            compatibility with the pachyderm dash) whose access level is
            queried. To query the access level of a robot user, the caller must
            prefix username with "robot:". If 'username' has no prefix (i.e.
            no ":"), then it's assumed to be a github user's principal.
        repo : str
            A string specifying the object to which `username`s access level is
            being granted/revoked.
        scope : int
            The access level that `username` will now have. See the ``Scope``
            enum for variants.
        """
        return self._req(
            Service.AUTH, "SetScope", username=username, repo=repo, scope=scope
        )

    def get_acl(self, repo):
        """Gets the ACL of a repo. Returns a ``GetACLResponse`` object.

        Parameters
        ----------
        repo : str
            The repo to get an ACL for.
        """
        return self._req(Service.AUTH, "GetACL", repo=repo)

    def set_acl(self, repo, entries):
        """Sets the ACL of a repo.

        Parameters
        ----------
        repo : str
            The repo to set an ACL on.
        entries : List[ACLEntry protobuf]
            A list of `ACLEntry` objects.
        """
        return self._req(Service.AUTH, "SetACL", repo=repo, entries=entries)

    def get_auth_token(self, subject, ttl=None):
        """Gets an auth token for a subject. Returns an
        ``GetAuthTokenResponse`` object.

        Parameters
        ----------
        subject : str
            The returned token will allow the caller to access resources as
            this subject.
        ttl : int, optional
            Indicates the approximate remaining lifetime of this token, in
            seconds.
        """
        return self._req(Service.AUTH, "GetAuthToken", subject=subject, ttl=ttl)

    def extend_auth_token(self, token, ttl):
        """Extends an existing auth token.

        Parameters
        ----------
        token : str
            Indicates the Pachyderm token whose TTL is being extended.
        ttl : int
            Indicates the approximate remaining lifetime of this token, in
            seconds.
        """
        return self._req(Service.AUTH, "ExtendAuthToken", token=token, ttl=ttl)

    def revoke_auth_token(self, token):
        """Revokes an auth token.

        Parameters
         ----------
         token : str
             Indicates the Pachyderm token that is being revoked.
        """
        return self._req(Service.AUTH, "RevokeAuthToken", token=token)

    def set_groups_for_user(self, username, groups):
        """Sets the group membership for a user.

        Parameters
        ----------
        username : str
            The username.
        groups : List[str]
            The groups to add `username` to.
        """
        return self._req(
            Service.AUTH, "SetGroupsForUser", username=username, groups=groups
        )

    def modify_members(self, group, add=None, remove=None):
        """Adds and/or removes members of a group.

        Parameters
        ----------
        group : str
            The group to modify.
        add : List[str], optional
            A list of strings specifying members to add.
        remove : List[str], optional
            A list of strings specifying members to remove.
        """
        return self._req(
            Service.AUTH,
            "ModifyMembers",
            group=group,
            add=add or [],
            remove=remove or [],
        )

    def get_groups(self, username=None):
        """Gets which groups the given `username` belongs to. Returns a list of
        strings.

        Parameters
        ----------
        username : str, optional
            The username.
        """
        return self._req(Service.AUTH, "GetGroups", username=username).groups

    def get_users(self, group):
        """Gets which users below to the `given`. Returns a list of strings.

        Parameters
        ----------
        group : str
            The group to list users for.
        """
        return self._req(Service.AUTH, "GetUsers", group=group).usernames

    def get_one_time_password(self, subject=None, ttl=None):
        """If this :class:`.Client` is authenticated as an admin, you can
        generate a one-time password for any given `subject`. If the caller is
        not an admin or the `subject` is not set, a one-time password will be
        returned for logged-in subject. Returns a string.

        Parameters
        ----------
        subject : str, optional
            The subject.
        ttl : int, optional
            Indicates the approximate remaining lifetime of this token, in
            seconds.
        """
        return self._req(
            Service.AUTH, "GetOneTimePassword", subject=subject, ttl=ttl
        ).code

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
