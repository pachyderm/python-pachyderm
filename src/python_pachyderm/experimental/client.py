from python_pachyderm import Client as _Client

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


class ExperimentalClient(
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
    _Client,
):
    def __init__(
        self,
        host: str = None,
        port: int = None,
        auth_token: str = None,
        root_certs: bytes = None,
        transaction_id: str = None,
        tls: bool = None,
        use_default_host: bool = True,
    ):
        _Client.__init__(
            self,
            host,
            port,
            auth_token,
            root_certs,
            transaction_id,
            tls,
            use_default_host,
        )
