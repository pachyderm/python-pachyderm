#!/usr/bin/env python3

import inspect
import string

from python_pachyderm.service import Service, GRPC_MODULES, PROTO_MODULES
from python_pachyderm import mixin

UPPERCASE = set(string.ascii_uppercase)

SERVICE_MIXINS = {
    Service.ADMIN: mixin.admin.AdminMixin,
    Service.PFS: mixin.pfs.PFSMixin,
    Service.PPS: mixin.pps.PPSMixin,
    Service.TRANSACTION: mixin.transaction.TransactionMixin,
    Service.VERSION: mixin.version.VersionMixin,
}

PROTO_OBJECT_BUILTINS = set([
    'ByteSize',
    'Clear',
    'ClearExtension',
    'ClearField',
    'CopyFrom',
    'DESCRIPTOR',
    'DiscardUnknownFields',
    'Extensions',
    'FindInitializationErrors',
    'FromString',
    'HasExtension',
    'HasField',
    'IsInitialized',
    'ListFields',
    'MergeFrom',
    'MergeFromString',
    'ParseFromString',
    'RegisterExtension',
    'SerializePartialToString',
    'SerializeToString',
    'SetInParent',
    'UnknownFields',
    'WhichOneof'
])

BLACKLISTED_METHODS = {
    Service.ADMIN: [],
    Service.PFS: ["delete_all"],
    Service.PPS: [],
    Service.TRANSACTION: ["delete_all"],
    Service.VERSION: ["get_version"],
}

WHITELISTED_ARGS = {
    Service.ADMIN: {
        "extract": ["url"],
        "restore": ["requests"],
    },
    Service.PFS: {
        "create_branch": ["commit", "repo_name"],
        "delete_branch": ["repo_name"],
        "delete_file": ["commit", "path"],
        "finish_commit": ["tree_object_hashes", "datum_object_hash"],
        "flush_commit": ["repos"],
        "get_file": ["commit", "path"],
        "inspect_branch": ["repo_name"],
        "inspect_file": ["commit", "path"],
        "list_commit": ["from_commit", "to_commit"],
        "list_file": ["commit", "path", "include_contents"],
        "subscribe_commit": ["from"],
        "walk_file": ["commit", "path"],
    },
    Service.PPS: {
    },
    Service.TRANSACTION: {
    },
    Service.VERSION: {
    },
}

BLACKLISTED_ARGS = {
    Service.ADMIN: {
        "extract": ["URL"],
        "restore": ["op", "URL"],
    },
    Service.PFS: {
        "create_branch": ["head"],
        "delete_file": ["file"],
        "delete_repo": ["all"],
        "finish_commit": ["trees", "datums"],
        "flush_commit": ["to_repos"],
        "get_file": ["file"],
        "inspect_file": ["commit", "path"],
        "list_commit": ["from", "to"],
        "list_file": ["file", "full"],
        "subscribe_commit": ["from_commit_id"],
        "walk_file": ["file"],
    },
    Service.PPS: {
    },
    Service.TRANSACTION: {
    },
    Service.VERSION: {
    },
}

def snake_to_camel(s):
    return "".join(x.capitalize() or "_" for x in s.split("_"))

def camel_to_snake(s):
    return s[0].lower() + "".join("_{}".format(c.lower()) if c in UPPERCASE else c for c in s[1:])

def names(obj):
    return [m for m in dir(obj) if not m.startswith("_")]

def trim_suffix(s, suffix):
    return s[:-len(suffix)] if s.endswith(suffix) else s

def lint(service, mixin, proto_module, grpc_module):
    mixin_method_names = set(names(mixin))
    grpc_cls = getattr(grpc_module, "APIServicer")
    grpc_method_names = set(names(grpc_cls))

    for grpc_method_name in grpc_method_names:
        if (not grpc_method_name.endswith("Stream")) and "{}Stream".format(grpc_method_name) in grpc_method_names:
            # skip methods with a streaming equivalent, since only the
            # streaming equivalent is implemented
            continue

        mixin_method_name = camel_to_snake(trim_suffix(grpc_method_name, "Stream"))

        if mixin_method_name not in mixin_method_names:
            if mixin_method_name not in BLACKLISTED_METHODS[service]:
                yield "missing method: {}".format(mixin_method_name)
            continue

        request_cls = getattr(proto_module, "{}Request".format(trim_suffix(grpc_method_name, "Stream")), None)
        mixin_method = getattr(mixin, mixin_method_name)
        mixin_method_args = set(a for a in inspect.getfullargspec(mixin_method).args if a != "self")

        if request_cls is None:
            if len(mixin_method_args) > 0:
                yield "method {}: unexpected arguments".format(mixin_method_name)
            continue

        request_args = set([n for n in names(request_cls) if n not in PROTO_OBJECT_BUILTINS])
        extra_args = mixin_method_args - request_args
        missing_args = request_args - mixin_method_args

        whitelisted_extra_args = set(WHITELISTED_ARGS[service].get(mixin_method_name, []))
        blacklisted_missing_args = set(BLACKLISTED_ARGS[service].get(mixin_method_name, []))
        for arg in extra_args:
            for suffix in ("_id", "_name"):
                trimmed = trim_suffix(arg, suffix)
                if arg != trimmed:
                    if trimmed in missing_args:
                        whitelisted_extra_args.add(arg)
                        blacklisted_missing_args.add(trimmed)
                    break

        for arg in extra_args - whitelisted_extra_args:
            yield "method {}: extra argument: {}".format(mixin_method_name, arg)
        for arg in missing_args - blacklisted_missing_args:
            yield "method {}: missing argument: {}".format(mixin_method_name, arg)

def main():
    for service in Service:
        service_mixin = SERVICE_MIXINS[service]
        proto_module = PROTO_MODULES[service]
        grpc_module = GRPC_MODULES[service]

        for warning in lint(service, service_mixin, proto_module, grpc_module):
            print("{}: {}".format(service.name, warning))

if __name__ == "__main__":
    main()
