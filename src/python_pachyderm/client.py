import os

from python_pachyderm.pfs import PFSMixin
from python_pachyderm.pps import PPSMixin
from python_pachyderm.transaction import TransactionMixin
from python_pachyderm.version import VersionMixin
from python_pachyderm.admin import AdminMixin


class Client(PFSMixin, PPSMixin, TransactionMixin, VersionMixin, AdminMixin, object):
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

    def _create_stub(self, grpc_module):
        if self.root_certs:
            ssl_channel_credentials = grpc_module.grpc.ssl_channel_credentials
            ssl = ssl_channel_credentials(root_certificates=self.root_certs)
            channel = grpc_module.grpc.secure_channel(self.address, ssl)
        else:
            channel = grpc_module.grpc.insecure_channel(self.address)
        return grpc_module.APIStub(channel)
