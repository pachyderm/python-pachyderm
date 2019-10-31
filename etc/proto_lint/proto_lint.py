#!/usr/bin/env python3

import inspect
import string

from python_pachyderm.service import Service, GRPC_MODULES, PROTO_MODULES
from python_pachyderm import mixin

# A set of uppercase characters
UPPERCASE = set(string.ascii_uppercase)

# Mapping of services to their implementing mixins
SERVICE_MIXINS = {
    Service.ADMIN: mixin.admin.AdminMixin,
    Service.PFS: mixin.pfs.PFSMixin,
    Service.PPS: mixin.pps.PPSMixin,
    Service.TRANSACTION: mixin.transaction.TransactionMixin,
    Service.VERSION: mixin.version.VersionMixin,
}

# Attributes of proto objects that are ignored, because they are built-in from
# the protobuf compiler
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

# A list of methods that we do not expect the library to implement
BLACKLISTED_METHODS = {
    Service.ADMIN: [],
    # delete_all is ignored because we implement PPS' delete_all anyway
    # put_file is ignored because we break it up into multiple functions
    # build_commit is ignored because it's for internal use only
    Service.PFS: ["delete_all", "put_file", "build_commit"],
    # activate_auth is ignored because it's an internal function
    # get_logs is ignored because we break it up into several functions
    # create_job is ignored because it's for internal use only
    Service.PPS: ["activate_auth", "get_logs", "create_job"],
    Service.TRANSACTION: ["delete_all"],
    # get_version is ignored because we renamed it to disambiguate
    Service.VERSION: ["get_version"],
}

# Extra arguments in python functions that should not show up as warnings,
# usually because they're just renamed versions of gRPC arguments
WHITELISTED_EXTRA_ARGS = {
    Service.ADMIN: {
        "restore": ["requests"],
    },
    Service.PFS: {
        "copy_file": ["source_commit", "source_path", "dest_commit", "dest_path"],
        "diff_file": ["new_commit", "new_path", "old_commit", "old_path"],
        "create_branch": ["commit"],
        "delete_branch": [],
        "finish_commit": ["tree_object_hashes", "datum_object_hash"],
        "flush_commit": ["repos"],
        "inspect_branch": [],
        "list_commit": ["from_commit", "to_commit"],
        "list_file": ["include_contents"],
        "start_commit": ["repo_name"],
        "subscribe_commit": ["from_commit_id"],
    },
    Service.PPS: {
        "inspect_pipeline": ["history"],
        "flush_job": ["pipeline_names"],
    },
    Service.TRANSACTION: {},
    Service.VERSION: {},
}

# Arguments in gRPC functions that aren't in the python functions, but should
# not show up as warnings, usually because they're just renamed in the library
BLACKLISTED_MISSING_ARGS = {
    Service.ADMIN: {
        "extract": ["URL"],
        "restore": ["op", "URL"],
    },
    Service.PFS: {
        "copy_file": ["src", "dst"],
        "diff_file": ["old_file", "new_file"],
        "create_branch": ["head", "s_branch"],
        "delete_file": ["file"],
        "delete_repo": ["all"],
        "finish_commit": ["trees", "datums"],
        "flush_commit": ["to_repos"],
        "get_file": ["file"],
        "list_commit": ["from", "to"],
        "list_file": ["file", "full"],
        "start_commit": ["repo_name"],
        "subscribe_commit": ["from"],
        "walk_file": ["file"],
    },
    Service.PPS: {
        "create_pipeline": ["pod_spec"],
        "delete_pipeline": ["all"],
        "flush_job": ["to_pipelines"],
        "list_pipeline": ["pipeline"],
    },
    Service.TRANSACTION: {},
    Service.VERSION: {},
}

# A mapping of python argument(s) to gRPC arguments. The python argument(s)
# can be considered a safe substitute for the gRPC arguments.
ARG_MAPPING = [
    (["repo_name"], "repo"),
    (["url"], "URL"),
    (["commit", "path"], "file"),
    (["pipeline_name"], "pipeline"),
    (["repo_name", "branch_name"], "branch"),
    (["datum_id", "job_id"], "datum"),
    (["job_id"], "job"),
]

def camel_to_snake(s):
    """Converts CamelCase strings to snake_case"""
    return s[0].lower() + "".join("_{}".format(c.lower()) if c in UPPERCASE else c for c in s[1:])

def attrs(obj):
    """Gets the non-private attributes of an object"""
    return [m for m in dir(obj) if not m.startswith("_")]

def trim_suffix(s, suffix):
    """Removes a suffix from a string if it exists"""
    return s[:-len(suffix)] if s.endswith(suffix) else s

def lint(service, mixin, proto_module, grpc_module):
    """Lints a given service"""

    mixin_method_names = set(attrs(mixin))
    grpc_cls = getattr(grpc_module, "APIServicer")
    grpc_method_names = set(attrs(grpc_cls))

    for grpc_method_name in grpc_method_names:
        if (not grpc_method_name.endswith("Stream")) and "{}Stream".format(grpc_method_name) in grpc_method_names:
            # skip methods with a streaming equivalent, since only the
            # streaming equivalent is implemented
            continue

        # get the equivalent mixin method name
        mixin_method_name = camel_to_snake(trim_suffix(grpc_method_name, "Stream"))

        # ignore blacklisted methods
        if mixin_method_name not in mixin_method_names:
            if mixin_method_name not in BLACKLISTED_METHODS[service]:
                yield "missing method: {}".format(mixin_method_name)
            continue

        # get the mixin function and its arguments
        request_cls = getattr(proto_module, "{}Request".format(trim_suffix(grpc_method_name, "Stream")), None)
        mixin_method = getattr(mixin, mixin_method_name)
        mixin_method_args = set(a for a in inspect.getfullargspec(mixin_method).args if a != "self")

        # give a warning for a python function that takes in arguments even
        # though there's no request object for the gRPC function, implying
        # that the gRPC function takes no arguments
        if request_cls is None:
            if len(mixin_method_args) > 0:
                yield "method {}: unexpected arguments".format(mixin_method_name)
            continue

        # find which arguments differ between the python and gRPC implementation
        request_args = set([n for n in attrs(request_cls) if n not in PROTO_OBJECT_BUILTINS])
        extra_args = mixin_method_args - request_args
        missing_args = request_args - mixin_method_args

        # find which differing arguments we can safely ignore
        ok_extra_args = set(WHITELISTED_EXTRA_ARGS[service].get(mixin_method_name, []))
        ok_missing_args = set(BLACKLISTED_MISSING_ARGS[service].get(mixin_method_name, []))
        for arg in missing_args:
            for (from_args, to_arg) in ARG_MAPPING:
                if arg == to_arg and all(a in extra_args for a in from_args):
                    ok_extra_args.update(from_args)
                    ok_missing_args.add(to_arg)

        # yield warnings for the remaining differing arguments
        for arg in extra_args - ok_extra_args:
            yield "method {}: extra argument: {}".format(mixin_method_name, arg)
        for arg in missing_args - ok_missing_args:
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
