"""
Exposes a mixin for each pachyderm service. These mixins should not be used
directly; instead, you should use `python_pachyderm.Client`. The mixins exist
exclusively in order to provide better code organization (because we have
several mixins, rather than one giant `Client` class.)
"""

from .pfs import PFSMixin
from .pps import PPSMixin
from .transaction import TransactionMixin
from .version import VersionMixin
from .admin import AdminMixin
