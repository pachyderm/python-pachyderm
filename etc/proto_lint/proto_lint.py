#!/usr/bin/env python3

"""
A linter that helps ensure we maintain parity between the protobuf definitions
and what's available in the higher-level `Client`. We have this because I got
sick of manually working my way through all the protobufs on every release.
"""

import sys
import inspect
import string

from python_pachyderm.service import Service
from python_pachyderm import mixin

# A set of uppercase characters
UPPERCASE = set(string.ascii_uppercase)

# Mapping of services to their implementing mixins
SERVICE_MIXINS = {
    Service.ADMIN: mixin.admin.AdminMixin,
    Service.AUTH: mixin.auth.AuthMixin,
    Service.DEBUG: mixin.debug.DebugMixin,
    Service.ENTERPRISE: mixin.enterprise.EnterpriseMixin,
    Service.HEALTH: mixin.health.HealthMixin,
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
    Service.PFS: [
        # delete_all is ignored because we implement PPS' delete_all anyway
        "delete_all",
        # the following are ignored because they're for internal use only
        "build_commit",
        "put_tar",
        "get_tar",
        # ignore storage v2 for now
        "get_tar_conditional_v2",
        "file_operation_v2",
        "list_file_v2",
        "glob_file_v2",
        "get_tar_v2",
        "walk_file_v2",
        "inspect_file_v2",
        "clear_commit_v2",
        "diff_file_v2",
    ],
    Service.PPS: [
        # the following are ignored because they're for internal use only
        "activate_auth",
        "create_job",
        "update_job_state",
    ],
}

# Mapping of what the linter would by default expect a proto name to be, to
# it's actual name
RENAMED_METHODS = {
    Service.AUTH: {
        "activate": ["activate_auth"],
        "deactivate": ["deactivate_auth"],
        "authenticate": ["authenticate_github", "authenticate_oidc", "authenticate_one_time_password"],
        "get_a_c_l": ["get_acl"],
        "set_a_c_l": ["set_acl"],
        "get_o_i_d_c_login": ["get_oidc_login"],
        "get_configuration": ["get_auth_configuration"],
        "set_configuration": ["set_auth_configuration"],
    },
    Service.DEBUG: {
        "profile": ["profile_cpu"],
    },
    Service.ENTERPRISE: {
        "activate": ["activate_enterprise"],
        "get_state": ["get_enterprise_state"],
        "deactivate": ["deactivate_enterprise"],
    },
    Service.PFS: {
        "put_file": ["put_file_bytes", "put_file_url"],
    },
    Service.PPS: {
        "get_logs": ["get_job_logs", "get_pipeline_logs"],
    },
    Service.TRANSACTION: {
        "delete_all": ["delete_all_transactions"],
    },
    Service.VERSION: {
        "get_version": ["get_remote_version"],
    }
}

# Mapping of renamed method arguments. Multiple times of remappings are
# supported:
# * An argument can simply be renamed: `("old_arg_name", "new_arg_name")`
# * An argument can be renamed to multiple arguments:
#   `("old_arg_name", ("new_arg_name_1", ("new_arg_name_2"))`
# * Multiple arguments can be renamed to a single argument:
#   `(("old_arg_name_1", "old_arg_name_2"), "new_arg_name")`
# * We can specify that a method isn't expected to have an argument:
#   `("ignored_arg_name", None)`
# * We can also specify that a method has an argument that isn't in the
#   protos: `(None, "ignored_arg_name")`
RENAMED_ARGS = {
    # admin
    "extract_pipeline": [
        ("pipeline", "pipeline_name"),
    ],
    "extract": [
        ("URL", "url"),
    ],
    "restore": [
        (("op", "URL"), "requests"),
    ],
    # auth
    "authenticate_github": [
        ("one_time_password", None),
        ("oidc_state", None),
        ("id_token", None),
    ],
    "authenticate_one_time_password": [
        ("github_token", None),
        ("oidc_state", None),
        ("id_token", None),
    ],
    "authenticate_oidc": [
        ("github_token", None),
        ("one_time_password", None),
        ("id_token", None),
    ],
    # debug
    "profile_cpu": [
        ("profile", None),
        (None, "duration"),
    ],
    # PFS
    "create_repo": [
        ("repo", "repo_name"),
    ],
    "create_branch": [
        ("branch", ("repo_name", "branch_name")),
        ("head", "commit"),
        ("s_branch", None),
    ],
    "create_secret": [
        ("file", ("secret_name", "data", "labels", "annotations")),
    ],
    "copy_file": [
        ("src", ("source_commit", "source_path")),
        ("dst", ("dest_commit", "dest_path")),
    ],
    "delete_branch": [
        ("branch", ("repo_name", "branch_name")),
    ],
    "delete_file": [
        ("file", ("commit", "path")),
    ],
    "delete_repo": [
        ("repo", "repo_name"),
        ("all", None),
    ],
    "diff_file": [
        ("old_file", ("old_commit", "old_path")),
        ("new_file", ("new_commit", "new_path")),
    ],
    "finish_commit": [
        ("tree", "input_tree_object_hash"),
        ("trees", "tree_object_hashes"),
        ("datums", "datum_object_hash"),
    ],
    "flush_commit": [
        ("to_repos", "repos"),
    ],
    "get_file": [
        ("file", ("commit", "path")),
    ],
    "inspect_branch": [
        ("branch", ("repo_name", "branch_name")),
    ],
    "inspect_commit": [
        ("repo", "repo_name"),
    ],
    "inspect_file": [
        ("file", ("commit", "path")),
    ],
    "inspect_repo": [
        ("repo", "repo_name"),
    ],
    "inspect_secret": [
        ("secret", "secret_name"),
    ],
    "list_branch": [
        ("repo", "repo_name"),
    ],
    "list_commit": [
        ("from", "from_commit"),
        ("to", "to_commit"),
        ("repo", "repo_name"),
    ],
    "list_file": [
        ("full", "include_contents"),
        ("file", ("commit", "path")),
    ],
    "put_file_bytes": [
        ("file", ("commit", "path")),
        ("url", None),
        ("recursive", None),
        ("delete", None),
    ],
    "put_file_url": [
        ("file", ("commit", "path")),
        ("value", None),
        ("delete", None),
    ],
    "start_commit": [
        ("parent", ("repo_name", "parent")),
    ],
    "subscribe_commit": [
        ("from", "from_commit_id"),
        ("repo", "repo_name"),
    ],
    "walk_file": [
        ("file", ("commit", "path")),
    ],
    # PPS
    "create_pipeline": [
        ("pipeline", "pipeline_name"),
        ("pod_spec", None),
        ("tf_job", None),
    ],
    "create_tf_job_pipeline": [
        ("pipeline", "pipeline_name"),
        ("pod_spec", None),
        ("transform", None),
    ],
    "delete_job": [
        ("job", "job_id"),
    ],
    "delete_pipeline": [
        ("pipeline", "pipeline_name"),
        ("all", None),
    ],
    "delete_secret": [
        ("secret", "secret_name"),
    ],
    "flush_job": [
        ("to_pipelines", "pipeline_names"),
    ],
    "get_job_logs": [
        ("job", "job_id"),
        ("master", None),
        ("pipeline", None),
    ],
    "get_pipeline_logs": [
        ("pipeline", ("pipeline_name")),
        ("job", None),
    ],
    "inspect_datum": [
        ("datum", ("job_id", "datum_id")),
    ],
    "inspect_job": [
        ("job", "job_id"),
    ],
    "inspect_pipeline": [
        ("pipeline", "pipeline_name"),
        (None, "history"),
    ],
    "list_datum": [
        ("job", "job_id"),
    ],
    "list_pipeline": [
        ("pipeline", None),
    ],
    "list_job": [
        ("pipeline", "pipeline_name"),
    ],
    "restart_datum": [
        ("job", "job_id"),
    ],
    "run_pipeline": [
        ("pipeline", "pipeline_name"),
    ],
    "run_cron": [
        ("pipeline", "pipeline_name"),
    ],
    "start_pipeline": [
        ("pipeline", "pipeline_name"),
    ],
    "stop_job": [
        ("job", "job_id"),
    ],
    "stop_pipeline": [
        ("pipeline", "pipeline_name"),
    ],
}

def camel_to_snake(s):
    """Converts CamelCase strings to snake_case"""
    return s[0].lower() + "".join("_{}".format(c.lower()) if c in UPPERCASE else c for c in s[1:])

def attrs(obj):
    """Gets the non-private attributes of an object"""
    return [m for m in dir(obj) if not m.startswith("_")]

def trim_suffix(s, suffix):
    """Removes a suffix from a string if it exists"""
    return s[:-len(suffix)] if s.endswith(suffix) else s

def args_set(values):
    s = set()

    for v in values:
        if v is not None:
            if isinstance(v, tuple):
                s.update(v)
            else:
                s.add(v)

    return s

def lint_method(mixin_cls, proto_module, grpc_method_name, mixin_method_name):
    # get the mixin function and its arguments
    request_cls = getattr(proto_module, "{}Request".format(trim_suffix(grpc_method_name, "Stream")), None)
    mixin_method = getattr(mixin_cls, mixin_method_name)
    mixin_method_args = set(a for a in inspect.getfullargspec(mixin_method).args if a != "self")

    # give a warning for a python function that takes in arguments even
    # though there's no request object for the gRPC function, implying
    # that the gRPC function takes no arguments
    if request_cls is None:
        if len(mixin_method_args) > 0:
            yield "method {}: unexpected arguments".format(mixin_method_name)
        return

    # find which arguments differ between the python and gRPC implementation
    request_args = set([n for n in attrs(request_cls) if n not in PROTO_OBJECT_BUILTINS])
    missing_args = request_args - mixin_method_args
    extra_args = mixin_method_args - request_args

    # find which differing arguments we can safely ignore
    ok_missing_args = args_set(s for (s, _) in RENAMED_ARGS.get(mixin_method_name, []))
    ok_extra_args = args_set(s for (_, s) in RENAMED_ARGS.get(mixin_method_name, []))

    # yield warnings for the remaining differing arguments
    for arg in extra_args - ok_extra_args:
        yield "method {}: extra argument: {}".format(mixin_method_name, arg)
    for arg in missing_args - ok_missing_args:
        yield "method {}: missing argument: {}".format(mixin_method_name, arg)

def lint_service(service):
    """Lints a given service"""

    mixin_cls = SERVICE_MIXINS[service]
    mixin_method_names = set(attrs(mixin_cls))
    proto_module = service.proto_module
    grpc_cls = service.servicer
    grpc_method_names = set(attrs(grpc_cls))

    for grpc_method_name in grpc_method_names:
        if (not grpc_method_name.endswith("Stream")) and "{}Stream".format(grpc_method_name) in grpc_method_names:
            # skip methods with a streaming equivalent, since only the
            # streaming equivalent is implemented
            continue

        # get the equivalent mixin method name
        mixin_method_name = camel_to_snake(trim_suffix(grpc_method_name, "Stream"))

        # ignore blacklisted methods
        if mixin_method_name in BLACKLISTED_METHODS.get(service, []):
            continue

        # find if this method is renamed
        renamed_mixin_method_names = RENAMED_METHODS.get(service, {}).get(mixin_method_name, [mixin_method_name])

        for mixin_method_name in renamed_mixin_method_names:
            # find if this method isn't implemented
            if mixin_method_name not in mixin_method_names:
                yield "service {}: missing method: {}".format(service.name, mixin_method_name)
                continue

            for warning in lint_method(mixin_cls, proto_module, grpc_method_name, mixin_method_name):
                yield "service {}: {}".format(service.name, warning)

def main():
    warned = False

    for service in Service:
        for warning in lint_service(service):
            print(warning)
            warned = True

    sys.exit(1 if warned else 0)

if __name__ == "__main__":
    main()
