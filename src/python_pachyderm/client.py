import os
import collections
import itertools
from contextlib import contextmanager
from urllib.parse import urlparse

from .mixin.pfs import PFSMixin
from .mixin.pps import PPSMixin
from .mixin.transaction import TransactionMixin
from .mixin.version import VersionMixin
from .mixin.admin import AdminMixin
from python_pachyderm.service import GRPC_MODULES, PROTO_MODULES


class Client(PFSMixin, PPSMixin, TransactionMixin, VersionMixin, AdminMixin, object):
    def __init__(self, host=None, port=None, auth_token=None, root_certs=None):
        """
        Creates a Pachyderm client.

        Params:

        * `host`: The pachd host. Default is 'localhost', which is used with
        `pachctl port-forward`.
        * `port`: The port to connect to. Default is 30650.
        * `auth_token`: The authentication token; used if authentication is
        enabled on the cluster. Defaults to `None`.
        * `root_certs`: The PEM-encoded root certificates as byte string.
        """

        host = host or "localhost"
        port = port or 30650
        self.address = "{}:{}".format(host, port)

        if auth_token is None:
            auth_token = os.environ.get("PACH_PYTHON_AUTH_TOKEN")

        self.metadata = []
        if auth_token is not None:
            self.metadata.append(("authn-token", auth_token))

        self.root_certs = root_certs
        self._stubs = {}

    @classmethod
    def new_in_cluster(cls, auth_token=None, root_certs=None):
        """
        Creates a Pachyderm client that operates within a Pachyderm cluster.

        Params:

        * `auth_token`: The authentication token; used if authentication is
        enabled on the cluster. Default to `None`.
        * `root_certs`: The PEM-encoded root certificates as byte string.
        """

        host = os.environ["PACHD_SERVICE_HOST"]
        port = int(os.environ["PACHD_SERVICE_PORT"])
        return cls(host=host, port=port, auth_token=auth_token, root_certs=root_certs)

    @classmethod
    def new_from_pachd_address(cls, pachd_address, auth_token=None, root_certs=None):
        """
        Creates a Pachyderm client from a given pachd address.

        Params:

        * `auth_token`: The authentication token; used if authentication is
        enabled on the cluster. Default to `None`.
        * `root_certs`: The PEM-encoded root certificates as byte string. This
        is required if the pachd address implies TLS is enabled.
        """

        if "://" not in pachd_address:
            pachd_address = "grpc://{}".format(pachd_address)

        u = urlparse(pachd_address)

        if u.scheme not in ("grpc", "http", "grpcs", "https"):
            raise ValueError("unrecognized pachd address scheme: {}".format(u.scheme))
        if (u.scheme == "grpcs" or u.scheme == "https") and root_certs is None:
            raise ValueError("the pachd address scheme implies TLS, but root_certs aren't set")
        if u.path != "" or u.params != "" or u.query != "" or u.fragment != "":
            raise ValueError("invalid pachd address")
        if u.username is not None or u.password is not None:
            raise ValueError("invalid pachd address")

        return cls(host=u.hostname, port=u.port, auth_token=auth_token, root_certs=root_certs)

    def _req(self, grpc_service, grpc_method_name, req=None, **kwargs):
        stub = self._stubs.get(grpc_service)
        if stub is None:
            grpc_module = GRPC_MODULES[grpc_service]
            if self.root_certs:
                ssl_channel_credentials = grpc_module.grpc.ssl_channel_credentials
                ssl = ssl_channel_credentials(root_certificates=self.root_certs)
                channel = grpc_module.grpc.secure_channel(self.address, ssl)
            else:
                channel = grpc_module.grpc.insecure_channel(self.address)
            stub = grpc_module.APIStub(channel)
            self._stubs[grpc_service] = stub

        assert req is None or len(kwargs) == 0

        if req is None:
            proto_module = PROTO_MODULES[grpc_service]
            if grpc_method_name.endswith("Stream"):
                req_cls_name_prefix = grpc_method_name[:-6]
            else:
                req_cls_name_prefix = grpc_method_name
            req_cls = getattr(proto_module, "{}Request".format(req_cls_name_prefix))
            req = req_cls(**kwargs)

        grpc_method = getattr(stub, grpc_method_name)
        return grpc_method(req, metadata=self.metadata)
