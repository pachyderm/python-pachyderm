from os import environ
from typing import Optional

from grpclib.client import Channel
from grpclib.exceptions import GRPCError

from python_pachyderm import Client as _Client
from python_pachyderm.errors import AuthServiceNotActivated
from .mixin import _synchronizer
from .mixin.admin import AdminApi
from .mixin.auth import AuthApi
from .mixin.debug import DebugApi
from .mixin.enterprise import EnterpriseApi
from .mixin.identity import IdentityApi
from .mixin.license import LicenseApi
from .mixin.pfs import PFSApi
from .mixin.pps import PPSApi
from .mixin.transaction import TransactionApi
from .mixin.version import VersionApi

AUTH_TOKEN_KEY = "authn-token"
AUTH_TOKEN_VAR = "PACH_PYTHON_AUTH_TOKEN"
OIDC_TOKEN_VAR = "PACH_PYTHON_OIDC_TOKEN"


class Client:
    """An experimental Client. New ``python_pachyderm`` features are available
    here first. Refer to the :class:`.Introduction` section in the Experimental
    Apis doc page to see the existing experimental prototypes Initialize an
    instance with ``python_pachyderm.experimental.Client()``.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        auth_token: Optional[str] = None,
        root_certs: Optional[str] = None,
        tls: bool = False,
        use_default_host: bool = True,
    ):
        if host is None and port is None and use_default_host:
            config = _Client._check_for_config()

            if config is not None:
                (
                    host,
                    port,
                    _,
                    auth_token,
                    root_certs,
                    transaction_id,
                    tls,
                ) = _Client._parse_config(config)

        host = host or "localhost"
        port = port or 30650

        if auth_token is None:
            auth_token = environ.get(AUTH_TOKEN_VAR)

        tls |= bool(root_certs)
        self._channel = Channel(host, port, ssl=tls)
        if root_certs:
            self._channel._ssl.load_verify_locations(cadata=root_certs)

        self._metadata = dict()
        self.admin = AdminApi(self._channel, metadata=self._metadata)
        self.auth = AuthApi(self._channel, metadata=self._metadata)
        self.debug = DebugApi(self._channel, metadata=self._metadata)
        self.enterprise = EnterpriseApi(self._channel, metadata=self._metadata)
        self.identity = IdentityApi(self._channel, metadata=self._metadata)
        self.license = LicenseApi(self._channel, metadata=self._metadata)
        self.pfs = PFSApi(self._channel, metadata=self._metadata)
        self.pps = PPSApi(self._channel, metadata=self._metadata)
        self.transaction = TransactionApi(self._channel, metadata=self._metadata)
        self.version = VersionApi(self._channel, metadata=self._metadata)

        # Do authentication if OIDC token provided without auth token.
        id_token = environ.get(OIDC_TOKEN_VAR)
        if not auth_token and id_token:
            auth_token = self.auth.authenticate_id_token(id_token)

        if auth_token:
            self._metadata[AUTH_TOKEN_KEY] = auth_token

    @property
    def address(self) -> str:
        return f"{self._channel._host}:{self._channel._port}"

    def delete_all(self) -> None:
        """Delete all repos, commits, files, pipelines, and jobs. This resets
        the cluster to its initial state.
        """
        # Try removing all identities if auth is activated.
        try:
            self.identity.delete_all()
        except (AuthServiceNotActivated, GRPCError):
            pass

        # Try deactivating auth if activated.
        try:
            self.auth.deactivate()
        except (AuthServiceNotActivated, GRPCError):
            pass

        # Try removing all licenses if auth is activated.
        try:
            self.license.delete_all()
        except (AuthServiceNotActivated, GRPCError):
            pass

        self.pps.delete_all_pipelines()
        self.pfs.delete_all_repos()
        self.transaction.delete_all()

    @_synchronizer
    async def health_check(self):
        from grpc_health.v1 import health_pb2
        from betterproto.lib.google.protobuf import Empty

        return await self.debug._unary_unary(
            "/grpc.health.v1.Health/Check",
            Empty(),
            health_pb2.HealthCheckResponse,
        )
