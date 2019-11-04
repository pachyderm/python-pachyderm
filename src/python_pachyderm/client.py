import os

from .mixin.pfs import PFSMixin
from .mixin.pps import PPSMixin
from .mixin.transaction import TransactionMixin
from .mixin.version import VersionMixin
from .mixin.admin import AdminMixin
from .mixin.debug import DebugMixin
from .mixin.health import HealthMixin


class Client(PFSMixin, PPSMixin, TransactionMixin, VersionMixin, AdminMixin, DebugMixin, HealthMixin, object):
    def __init__(self, host=None, port=None, auth_token=None, root_certs=None):
        """
        Creates a client to connect to PFS.

        Params:

        * `host`: The pachd host. Default is 'localhost', which is used with
        `pachctl port-forward`.
        * `port`: The port to connect to. Default is 30650.
        * `auth_token`: The authentication token; used if authentication is
        enabled on the cluster. Defaults to `None`.
        * `root_certs`:  The PEM-encoded root certificates as byte string.
        """

        if host is not None and port is not None:
            self.address = "{}:{}".format(host, port)
        else:
            self.address = os.environ.get("PACHD_ADDRESS", "localhost:30650")

        if auth_token is None:
            auth_token = os.environ.get("PACH_PYTHON_AUTH_TOKEN")

        self.metadata = []
        if auth_token is not None:
            self.metadata.append(("authn-token", auth_token))

        self.root_certs = root_certs
        self._stubs = {}

    def _req(self, grpc_service, grpc_method_name, req=None, **kwargs):
        stub = self._stubs.get(grpc_service)
        if stub is None:
            grpc_module = grpc_service.grpc_module
            if self.root_certs:
                ssl_channel_credentials = grpc_module.grpc.ssl_channel_credentials
                ssl = ssl_channel_credentials(root_certificates=self.root_certs)
                channel = grpc_module.grpc.secure_channel(self.address, ssl)
            else:
                channel = grpc_module.grpc.insecure_channel(self.address)
            stub = grpc_service.stub(channel)
            self._stubs[grpc_service] = stub

        assert req is None or len(kwargs) == 0

        if req is None:
            proto_module = grpc_service.proto_module
            if grpc_method_name.endswith("Stream"):
                req_cls_name_prefix = grpc_method_name[:-6]
            else:
                req_cls_name_prefix = grpc_method_name
            req_cls = getattr(proto_module, "{}Request".format(req_cls_name_prefix))
            req = req_cls(**kwargs)

        grpc_method = getattr(stub, grpc_method_name)
        return grpc_method(req, metadata=self.metadata)
