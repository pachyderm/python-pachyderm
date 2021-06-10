import re

from python_pachyderm.proto.v2.pfs import pfs_pb2 as pfs_proto

# copied from pachyderm/pachyderm
uuid_re = re.compile(r"[0-9a-f]{12}4[0-9a-f]{19}")
valid_branch_re = re.compile(r"^[a-zA-Z0-9_-]+$")


def commit_from(src):
    if src is None:
        return None
    if isinstance(src, pfs_proto.Commit):
        return src

    repo, branch, commit, repo_type = None, None, None, "user"
    if isinstance(src, (tuple, list)):
        if len(src) == 3:
            repo, branch, commit = src
        elif len(src) == 2:
            repo, branch_or_commit = src
            if uuid_re.match(branch_or_commit) or not valid_branch_re.match(
                branch_or_commit
            ):
                commit = branch_or_commit
            else:
                branch = branch_or_commit

    if repo is None:
        raise ValueError("Repo cannot be empty")

    return pfs_proto.Commit(
        id=commit,
        branch=pfs_proto.Branch(
            repo=pfs_proto.Repo(name=repo, type=repo_type), name=branch
        ),
    )
