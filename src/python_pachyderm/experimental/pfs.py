import re
from typing import NamedTuple, Union

from python_pachyderm.experimental.service import pfs_proto
from python_pachyderm.service import pfs_proto as pfs_proto_pb

# copied from pachyderm/pachyderm
valid_branch_re = re.compile(r"^[a-zA-Z0-9_-]+$")
uuid_re = re.compile(r"[0-9a-f]{12}4[0-9a-f]{19}")


class Commit(NamedTuple):
    """A namedtuple subclass to specify a Commit."""

    repo: str
    branch: str = None
    id: str = None
    repo_type: str = "user"

    def to_pb(self) -> pfs_proto_pb.Commit:
        """Converts itself into a Google protobuf ``Commit``."""
        return pfs_proto_pb.Commit(
            id=self.id,
            branch=pfs_proto_pb.Branch(
                repo=pfs_proto_pb.Repo(name=self.repo, type=self.repo_type),
                name=self.branch,
            ),
        )

    def to_bp(self) -> pfs_proto.Commit:
        """Converts itself into a Betterproto ``Commit``."""
        return pfs_proto.Commit(
            id=self.id,
            branch=pfs_proto.Branch(
                repo=pfs_proto.Repo(name=self.repo, type=self.repo_type),
                name=self.branch,
            ),
        )

    @staticmethod
    def from_bp(commit: pfs_proto.Commit) -> "Commit":
        """Converts a ``pfs_proto.Commit`` object into a :class:`.Commit`
        object.
        """
        return Commit(
            repo=commit.branch.repo.name,
            branch=commit.branch.name,
            id=commit.id,
            repo_type=commit.branch.repo.type,
        )

    @staticmethod
    def from_pb(commit: pfs_proto_pb.Commit) -> "Commit":
        """Converts a ``pfs_proto_pb.Commit`` object into a :class:`.Commit`
        object.
        """
        return Commit(
            repo=commit.branch.repo.name,
            branch=commit.branch.name,
            id=commit.id,
            repo_type=commit.branch.repo.type,
        )


def commit_from(
    commit: Union[tuple, dict, Commit, pfs_proto.Commit, pfs_proto_pb.Commit] = None,
) -> pfs_proto.Commit:
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
    if isinstance(commit, pfs_proto.Commit):
        return commit
    if isinstance(commit, pfs_proto_pb.Commit):
        return Commit.from_pb(commit).to_bp()
    if isinstance(commit, Commit):
        return commit.to_bp()
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
        ).to_bp()
    if isinstance(commit, dict):
        return Commit(**commit).to_bp()
    if commit is None:
        return None

    raise TypeError("Please provide a tuple, dict, or Commit object")
