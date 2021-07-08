import os
import json
from pathlib import Path
from urllib.parse import urlparse

from .mixin.admin import AdminMixin
from .mixin.auth import AuthMixin
from .mixin.debug import DebugMixin
from .mixin.enterprise import EnterpriseMixin
from .mixin.health import HealthMixin
from .mixin.identity import IdentityMixin
from .mixin.license import LicenseMixin
from .mixin.pfs import PFSMixin
from .mixin.pps import PPSMixin
from .mixin.transaction import TransactionMixin
from .mixin.version import VersionMixin
from .service import Service


class ConfigError(Exception):
    """Error for issues related to the pachyderm config file"""

    def __init__(self, message):
        super().__init__(message)


class BadClusterDeploymentID(ConfigError):
    """
    Error triggered when connected to a cluster that reports back a different
    cluster deployment ID than what is stored in the config file
    """

    def __init__(self, expected_deployment_id, actual_deployment_id):
        super().__init__(
            "connected to the wrong cluster ('{}' vs '{}')".format(
                expected_deployment_id, actual_deployment_id
            )
        )
        self.expected_deployment_id = expected_deployment_id
        self.actual_deployment_id = actual_deployment_id


class Client(
    AdminMixin,
    AuthMixin,
    DebugMixin,
    EnterpriseMixin,
    HealthMixin,
    IdentityMixin,
    LicenseMixin,
    PFSMixin,
    PPSMixin,
    TransactionMixin,
    VersionMixin,
    object,
):
    # Class variables for checking config
    env_config = "PACH_CONFIG"
    spout_config = "/pachctl/config.json"
    local_config = f"{Path.home()}/.pachyderm/config.json"

    def __init__(
        self,
        host=None,
        port=None,
        auth_token=None,
        root_certs=None,
        transaction_id=None,
        tls=None,
        use_default_host=True,
    ):
        """
        Creates a Pachyderm client. If host and port are unset, checks the
        PACH_CONFIG env var for a path. If that's unset, it checks two
        file paths for a config file. If both files don't exist, a client
        with default settings is created.

        Params:

        * `host`: The pachd host. Default is 'localhost', which is used with
        `pachctl port-forward`.
        * `port`: The port to connect to. Default is 30650.
        * `auth_token`: The authentication token. Used if authentication is
        enabled on the cluster. Defaults to `None`.
        * `root_certs`:  The PEM-encoded root certificates as byte string.
        * `transaction_id`: The ID of the transaction to run operations on.
        * `tls`: Specifies whether TLS should be used. If `root_certs` are
        specified, they are used; otherwise, we use the certs provided by
        certifi.
        * `use_default_host`: Boolean for replicating `pachctl` behavior
        of searching for config. Default is True.
        """

        # replicate pachctl behavior to searching for config
        # if host and port are unset
        if host is None and port is None and use_default_host:
            config = Client.check_for_config()

            if config is not None:
                (
                    host,
                    port,
                    _,
                    auth_token,
                    root_certs,
                    transaction_id,
                    tls,
                ) = Client._parse_config(config)

        host = host or "localhost"
        port = port or 30650

        if auth_token is None:
            auth_token = os.environ.get("PACH_PYTHON_AUTH_TOKEN")

        if tls is None:
            tls = root_certs is not None
        if tls and root_certs is None:
            # load default certs if none are specified
            import certifi

            with open(certifi.where(), "rb") as f:
                root_certs = f.read()

        self.address = "{}:{}".format(host, port)
        self.root_certs = root_certs
        self._stubs = {}
        self._auth_token = auth_token
        self._transaction_id = transaction_id
        self._metadata = self._build_metadata()
        if not auth_token and os.environ.get("PACH_PYTHON_OIDC_TOKEN"):
            resp = self.authenticate_id_token(os.environ.get("PACH_PYTHON_OIDC_TOKEN"))
            self._auth_token = resp
            self._metadata = self._build_metadata()

    @classmethod
    def new_in_cluster(cls, auth_token=None, transaction_id=None):
        """
        Creates a Pachyderm client that operates within a Pachyderm cluster.

        Params:

        * `auth_token`: The authentication token; used if authentication is
        enabled on the cluster. Default to `None`.
        * `transaction_id`: The ID of the transaction to run operations on.
        """

        if (
            "PACHD_PEER_SERVICE_HOST" in os.environ
            and "PACHD_PEER_SERVICE_PORT" in os.environ
        ):
            # Try to use the pachd peer service if it's available. This is
            # only supported in pachyderm>=1.10, but is more reliable because
            # it'll work when TLS is enabled on the cluster.
            host = os.environ["PACHD_PEER_SERVICE_HOST"]
            port = int(os.environ["PACHD_PEER_SERVICE_PORT"])
        else:
            # Otherwise use the normal service host/port, which will not work
            # when TLS is enabled on the cluster.
            host = os.environ["PACHD_SERVICE_HOST"]
            port = int(os.environ["PACHD_SERVICE_PORT"])

        return cls(
            host=host,
            port=port,
            auth_token=auth_token,
            transaction_id=transaction_id,
            use_default_host=False,
        )

    @classmethod
    def new_from_pachd_address(
        cls, pachd_address, auth_token=None, root_certs=None, transaction_id=None
    ):
        """
        Creates a Pachyderm client from a given pachd address.

        Params:

        * `pachd_address`: The address of pachd server.
        * `auth_token`: The authentication token. Used if authentication is
        enabled on the cluster. Default to `None`.
        * `root_certs`: The PEM-encoded root certificates as byte string. If
        unspecified, this will load default certs from certifi.
        * `transaction_id`: The ID of the transaction to run operations on.
        """

        u = Client._parse_address(pachd_address)

        return cls(
            host=u.hostname,
            port=u.port,
            auth_token=auth_token,
            root_certs=root_certs,
            transaction_id=transaction_id,
            tls=u.scheme == "grpcs" or u.scheme == "https",
            use_default_host=False,
        )

    @classmethod
    def new_from_config(cls, config_file):
        """
        Creates a Pachyderm client from a config file-like object.

        Params:

        * `config_file`: A file-like object containing the config
        json file.
        """

        if config_file is None:
            raise ConfigError("no config object provided")

        config = json.load(config_file)
        (
            _,
            _,
            pachd_address,
            auth_token,
            root_certs,
            transaction_id,
            _,
        ) = cls._parse_config(config)

        client = cls.new_from_pachd_address(
            pachd_address,
            auth_token=auth_token,
            root_certs=root_certs,
            transaction_id=transaction_id,
        )

        context = cls._get_active_context(config)
        expected_deployment_id = context.get("cluster_deployment_id")
        if expected_deployment_id:
            cluster_info = client.inspect_cluster()
            if cluster_info.deployment_id != expected_deployment_id:
                raise BadClusterDeploymentID(
                    expected_deployment_id, cluster_info.deployment_id
                )

        return client

    @staticmethod
    def check_for_config():
        """
        Checks for config.

        Returns:
        * `j`: The config as a json object.
        """

        j = Client.check_pach_config_env_var()
        if j is not None:
            return j

        j = Client.check_pach_config_spout()
        if j is not None:
            return j

        j = Client.check_pach_config_local()
        if j is not None:
            return j

        print("no config found, proceeding with default behavior")

        return j

    @staticmethod
    def check_pach_config_env_var():
        j = None
        if Client.env_config in os.environ:
            with open(os.environ.get(Client.env_config), "r") as config_file:
                j = json.load(config_file)

        return j

    @staticmethod
    def check_pach_config_spout():
        j = None
        if os.path.isfile(Client.spout_config):
            with open(Client.spout_config, "r") as config_file:
                j = json.load(config_file)

        return j

    @staticmethod
    def check_pach_config_local():
        j = None
        if os.path.isfile(Client.local_config):
            with open(Client.local_config, "r") as config_file:
                j = json.load(config_file)

        return j

    @staticmethod
    def _parse_address(pachd_address):
        if "://" not in pachd_address:
            pachd_address = "grpc://{}".format(pachd_address)

        u = urlparse(pachd_address)

        if u.scheme not in ("grpc", "http", "grpcs", "https"):
            raise ValueError("unrecognized pachd address scheme: {}".format(u.scheme))
        if u.path != "" or u.params != "" or u.query != "" or u.fragment != "":
            raise ValueError("invalid pachd address")
        if u.username is not None or u.password is not None:
            raise ValueError("invalid pachd address")

        return u

    @staticmethod
    def _get_active_context(config):
        try:
            active_context = config["v2"]["active_context"]
        except KeyError:
            raise ConfigError("no active context")

        try:
            context = config["v2"]["contexts"][active_context]
        except KeyError:
            raise ConfigError("missing active context '{}'".format(active_context))

        return context

    @staticmethod
    def _parse_config(config):
        context = Client._get_active_context(config)

        auth_token = context.get("session_token")
        root_certs = context.get("server_cas")
        transaction_id = context.get("active_transaction")

        pachd_address = context.get("pachd_address")
        if not pachd_address:
            port_forwarders = context.get("port_forwarders", {})
            pachd_port = port_forwarders.get("pachd", 30650)
            pachd_address = "grpc://localhost:{}".format(pachd_port)

            root_certs = None

        u = Client._parse_address(pachd_address)

        host = u.hostname
        port = u.port
        tls = u.scheme == "grpcs" or u.scheme == "https"

        return host, port, pachd_address, auth_token, root_certs, transaction_id, tls

    @property
    def auth_token(self):
        return self._auth_token

    @auth_token.setter
    def auth_token(self, value):
        self._auth_token = value
        self._metadata = self._build_metadata()

    @property
    def transaction_id(self):
        return self._transaction_id

    @transaction_id.setter
    def transaction_id(self, value):
        self._transaction_id = value
        self._metadata = self._build_metadata()

    def _build_metadata(self):
        metadata = []
        if self._auth_token is not None:
            metadata.append(("authn-token", self._auth_token))
        if self._transaction_id is not None:
            metadata.append(("pach-transaction", self._transaction_id))
        return metadata

    def _req(self, grpc_service: Service, grpc_method_name, req=None, **kwargs):
        stub = self._stubs.get(grpc_service)
        if stub is None:
            grpc_module = grpc_service.grpc_module
            if self.root_certs:
                ssl_channel_credentials = grpc_module.grpc.ssl_channel_credentials
                ssl = ssl_channel_credentials(root_certificates=self.root_certs)
                channel = grpc_module.grpc.secure_channel(
                    self.address,
                    ssl,
                    options=grpc_service.options,
                )
            else:
                channel = grpc_module.grpc.insecure_channel(
                    self.address,
                    options=grpc_service.options,
                )
            stub = grpc_service.stub(channel)
            self._stubs[grpc_service] = stub

        assert req is None or len(kwargs) == 0
        assert self._metadata is not None

        if req is None:
            proto_module = grpc_service.proto_module
            if grpc_method_name.endswith("Stream"):
                req_cls_name_prefix = grpc_method_name[:-6]
            else:
                req_cls_name_prefix = grpc_method_name
            req_cls = getattr(proto_module, "{}Request".format(req_cls_name_prefix))
            req = req_cls(**kwargs)

        grpc_method = getattr(stub, grpc_method_name)
        return grpc_method(req, metadata=self._metadata)

    def delete_all(self):
        self.delete_all_pipelines()
        self.delete_all_repos()
        self.delete_all_transactions()
