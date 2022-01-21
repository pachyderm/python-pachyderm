import re
from typing import NamedTuple, Optional, Union

from python_pachyderm.proto.v2.pfs import pfs_pb2

# copied from pachyderm/pachyderm
valid_branch_re = re.compile(r"^[a-zA-Z0-9_-]+$")
uuid_re = re.compile(r"[0-9a-f]{12}4[0-9a-f]{19}")


class Commit(NamedTuple):
    """A namedtuple subclass to specify a Commit."""

    repo: str
    branch: str = None
    id: str = None
    repo_type: str = "user"

    def to_pb(self) -> pfs_pb2.Commit:
        """Converts itself into a ``pfs_proto.Commit``."""
        return pfs_pb2.Commit(
            id=self.id,
            branch=pfs_pb2.Branch(
                repo=pfs_pb2.Repo(name=self.repo, type=self.repo_type),
                name=self.branch,
            ),
        )

    @staticmethod
    def from_pb(commit: pfs_pb2.Commit) -> "Commit":
        """Converts a ``pfs_proto.Commit`` object into a :class:`.Commit`
        object.
        """
        return Commit(
            repo=commit.branch.repo.name,
            branch=commit.branch.name,
            id=commit.id,
            repo_type=commit.branch.repo.type,
        )


COMMIT_LIKE = Union[tuple, dict, "Commit", pfs_pb2.Commit]


def commit_from(commit: Optional[COMMIT_LIKE] = None) -> pfs_pb2.Commit:
    """A commit can be identified by (repo, branch, commit_id, repo_type)

    Helper function to convert objects that represent a Commit query into a
    protobuf Commit object.

    Parameters
    ----------
    commit : Union[tuple, dict, Commit, pfs_proto.Commit], optional
        The commit representation to convert to a protobuf commit object.

    Returns
    -------
    pfs_proto.Commit
        A protobuf object that represents a commit.
    """
    if isinstance(commit, pfs_pb2.Commit):
        return commit
    if isinstance(commit, Commit):
        return commit.to_pb()
    if isinstance(commit, tuple):
        repo, branch, commit_id, repo_type = None, None, None, "user"
        if len(commit) == 2:
            repo, branch_or_commit = commit
            if uuid_re.match(branch_or_commit) or not valid_branch_re.match(
                branch_or_commit
            ):
                commit_id = branch_or_commit
            else:
                branch = branch_or_commit
        elif len(commit) == 3:
            repo, branch, commit_id = commit
        else:
            repo, branch, commit_id, repo_type = commit
        return Commit(
            repo=repo, branch=branch, id=commit_id, repo_type=repo_type
        ).to_pb()
    if isinstance(commit, dict):
        return Commit(**commit).to_pb()
    if commit is None:
        return None

    raise TypeError("Please provide a tuple, dict, or Commit object")
