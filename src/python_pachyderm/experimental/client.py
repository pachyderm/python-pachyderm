from enum import IntEnum
from typing import Callable, Iterator
import google
from google.protobuf import empty_pb2
import betterproto
import betterproto.lib.google.protobuf as bp_proto

from python_pachyderm import Client as _Client
from python_pachyderm.service import Service
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
from .service import BP_MODULES


class ProtoIterator:
    def __init__(self, stream: Iterator, conversion: Callable):
        self.stream = stream
        self.conversion = conversion

    def __next__(self):
        try:
            proto = next(self.stream)
        except StopIteration:
            raise StopIteration

        return self.conversion(proto)

    def __iter__(self):
        return self

    def cancel(self) -> None:
        self.stream.cancel()


# converts enum values (ints) to their actual enum objects
def _show_enums(bp_obj: betterproto.Message):
    for k, v_type in bp_obj._type_hints().items():
        if k[0] == "_":
            continue
        if "typing.List" in str(v_type):
            if "proto" not in str(v_type):
                continue

            proto_l = getattr_no_case(bp_obj, k)
            class_of_proto_in_list = v_type.__args__[0]

            if issubclass(class_of_proto_in_list, IntEnum):
                setattr(bp_obj, k, [class_of_proto_in_list(num) for num in proto_l])
            else:
                for obj in proto_l:
                    _show_enums(obj)
        elif "typing.Dict" in str(v_type):
            if "proto" not in str(v_type):
                continue

            proto_d = getattr_no_case(bp_obj, k)
            class_of_proto_in_dict = v_type.__args__[1]

            if issubclass(class_of_proto_in_dict, IntEnum):
                setattr(
                    bp_obj,
                    k,
                    {k2: class_of_proto_in_dict(proto_d[k2]) for k2 in proto_d.keys()},
                )
            else:
                for k2 in proto_d.keys():
                    _show_enums(proto_d[k2])
        elif "proto" in str(v_type):
            _show_enums(getattr_no_case(bp_obj, k))
        elif issubclass(v_type, IntEnum):
            setattr(bp_obj, k, v_type(getattr_no_case(bp_obj, k)))


def pb_to_bp(pb_obj: google.protobuf.descriptor):
    # GRPC returned empty protobuf
    if isinstance(pb_obj, empty_pb2.Empty):
        return bp_proto.Empty()

    # GRPC returned stream
    if hasattr(pb_obj, "__next__"):
        return ProtoIterator(pb_obj, pb_to_bp)

    # find corresponding betterproto class
    pb_obj_type = type(pb_obj).__name__
    try:
        # GRPC returned one of our custom protobufs
        proto = pb_obj.__module__.split(".")[-2]
        proto = "version" if proto == "versionpb" else proto

        grpc_service = getattr_no_case(Service, proto.upper())
        bp_class = getattr_no_case(BP_MODULES[grpc_service], pb_obj_type)
    except AttributeError:
        # GRPC returned a *Value protobuf
        bp_class = getattr_no_case(bp_proto, pb_obj_type)

    bp_obj = bp_class().parse(pb_obj.SerializeToString())
    _show_enums(bp_obj)

    return bp_obj


def bp_to_pb(bp_obj: betterproto.Message):
    if isinstance(bp_obj, bp_proto.Empty):
        return empty_pb2.Empty()

    if isinstance(bp_obj, IntEnum):
        return bp_obj

    # Input stream
    if hasattr(bp_obj, "__next__"):
        return ProtoIterator(bp_obj, bp_to_pb)

    # find corresponding protobuf class
    proto = bp_obj.__module__.split(".")[-1].split("_")[0]
    proto = "version" if proto == "versionpb" else proto

    grpc_service = getattr_no_case(Service, proto.upper())
    pb_class = getattr_no_case(grpc_service.proto_module, type(bp_obj).__name__)

    pb_obj = pb_class().FromString(bytes(bp_obj))

    return pb_obj


# case-insensitive getattr
def getattr_no_case(obj: object, attr: str) -> object:
    try:
        return getattr(obj, attr)
    except AttributeError:
        for a in dir(obj):
            if a.lower() == attr.lower():
                return getattr(obj, a)
        raise AttributeError


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
    _Client,
):
    """An experimental Client. New ``python_pachyderm`` features are available
    here first. Refer to the :class:`.Introduction` section in the Experimental
    Mixins doc page to see the existing experimental prototypes Initialize an
    instance with ``python_pachyderm.experimental.Client()``.
    """

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

    def _req(self, grpc_service: Service, grpc_method_name: str, req=None, **kwargs):
        # For proto request objects with no equivalent betterproto
        if "pb2" in type(req).__module__ or grpc_method_name == "ModifyFile":
            return _Client._req(self, grpc_service, grpc_method_name, req, **kwargs)

        # convert betterproto to google protobuf
        if req is not None:
            req = bp_to_pb(req)
        else:
            for k, v in kwargs.items():
                if (
                    isinstance(v, list)
                    and hasattr(v[0], "__module__")
                    and "proto" in v[0].__module__
                ):
                    kwargs[k] = [bp_to_pb(bp) for bp in kwargs[k]]
                elif hasattr(v, "__module__") and "proto" in v.__module__:
                    kwargs[k] = bp_to_pb(v)

        # gRPC call
        pb = _Client._req(self, grpc_service, grpc_method_name, req, **kwargs)

        return pb_to_bp(pb)
