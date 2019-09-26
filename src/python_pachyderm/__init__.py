import string as _string
import importlib as _importlib

def _import_protos(path, *enums):
    g = globals()
    module = _importlib.import_module(path)
    uppercase_letters = set(_string.ascii_uppercase)
    lowercase_letters = set(_string.ascii_lowercase)

    for key in dir(module):
        added = False

        for (enum_prefix, enum_members) in enums:
            if key in enum_members:
                if enum_prefix is not None:
                    g["{}_{}".format(enum_prefix, key)] = getattr(module, key)
                else:
                    g[key] = getattr(module, key)
                added = True
                break

        if not added and key[0] in uppercase_letters and any(c in lowercase_letters for c in key[1:]):
            g[key] = getattr(module, key)

_import_protos(
    "python_pachyderm.proto.pfs.pfs_pb2",
    ("FILE_TYPE", set(("RESERVED", "FILE", "DIR"))),
    ("FILE_DELIMITER", set(("NONE", "JSON", "LINE", "SQL", "CSV"))),
    ("ORIGIN_KIND", set(("USER", "AUTH", "FSCK"))),
    ("COMMIT_STATE", set(("STARTED", "READY", "FINISHED"))),
)

_import_protos(
    "python_pachyderm.proto.pps.pps_pb2",
    (None, set(("JOB_STARTING", "JOB_RUNNING", "JOB_FAILURE", "JOB_SUCCESS", "JOB_KILLED", "JOB_MERGING"))),
    ("DATUM_STATE", set(("FAILED", "SUCCESS", "SKIPPED", "STARTING", "RECOVERED"))),
    (None, set(("POD_RUNNING", "POD_SUCCESS", "POD_FAILED"))),
    (None, set(("PIPELINE_STARTING", "PIPELINE_RUNNING", "PIPELINE_RESTARTING", "PIPELINE_FAILURE", "PIPELINE_PAUSED", "PIPELINE_STANDBY"))),
)

_import_protos("python_pachyderm.proto.version.versionpb.version_pb2")

from .client import Client
from grpc import RpcError
