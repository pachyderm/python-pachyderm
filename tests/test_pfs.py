#!/usr/bin/env python

"""Tests PFS-related functionality"""
import os
import tempfile
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import pytest

import python_pachyderm
from python_pachyderm import Client, PFSFile
from python_pachyderm.service import pfs_proto, MAX_RECEIVE_MESSAGE_SIZE
from tests import util


def sandbox(test_name):
    client = python_pachyderm.Client()
    repo_name = util.create_test_repo(client, test_name)
    return client, repo_name


def test_inspect_repo():
    client, repo_name = sandbox("inspect_repo")
    client.inspect_repo(repo_name)
    repos = list(client.list_repo())
    assert len(repos) >= 1
    assert repo_name in [r.repo.name for r in repos]


def test_delete_repo():
    client, repo_name = sandbox("delete_repo")
    orig_repo_count = len(list(client.list_repo()))
    assert orig_repo_count >= 1
    client.delete_repo(repo_name)
    assert len(list(client.list_repo())) == orig_repo_count - 1


def test_delete_non_existent_repo():
    client = python_pachyderm.Client()
    orig_repo_count = len(list(client.list_repo()))
    client.delete_repo("BOGUS_NAME")
    assert len(list(client.list_repo())) == orig_repo_count


def test_delete_all_repos():
    client = python_pachyderm.Client()

    util.create_test_repo(client, "test_delete_all_repos", prefix="extra-1")
    util.create_test_repo(client, "test_delete_all_repos", prefix="extra-2")
    assert len(list(client.list_repo())) >= 2

    client.delete_all_repos()
    assert len(list(client.list_repo())) == 0


def test_start_commit():
    client, repo_name = sandbox("start_commit")

    commit = client.start_commit(repo_name, "master")
    assert commit.branch.repo.name == repo_name

    # cannot start new commit before the previous one is finished
    with pytest.raises(
        python_pachyderm.RpcError, match=r"parent commit .* has not been finished"
    ):
        client.start_commit(repo_name, "master")

    client.finish_commit(commit)
    commit2 = client.start_commit(repo_name, "master")
    assert commit2.branch.repo.name == repo_name

    with pytest.raises(python_pachyderm.RpcError, match=r"repo .* not found"):
        client.start_commit("some-fake-repo", "master")


def test_start_commit_on_branch_with_parent():
    client, repo_name = sandbox("start_commit_on_branch_with_parent")

    commit1 = client.start_commit(repo_name, branch="master")
    client.finish_commit(commit1)

    commit2 = client.start_commit(repo_name, branch="master", parent=commit1.id)
    assert commit2.branch.repo.name == repo_name


def test_start_commit_fork():
    client, repo_name = sandbox("start_commit_fork")

    commit1 = client.start_commit(repo_name, branch="master")
    client.finish_commit((repo_name, commit1.id))

    commit2 = client.start_commit(repo_name, branch="patch", parent="master")

    assert commit2.branch.repo.name == repo_name

    branches = [
        branch_info.branch.name for branch_info in list(client.list_branch(repo_name))
    ]
    assert "master" in branches
    assert "patch" in branches


@pytest.mark.parametrize(
    "commit_arg",
    [
        "commit_obj",
        "(repo, commit_id)",
        "(repo, branch)",
        "(repo, branch, commit_id)",
        "(repo, branch, commit_id, type)",
        "dictionary",
    ],
)
def test_finish_commit(commit_arg):
    client, repo_name = sandbox("finish_commit")
    commit = client.start_commit(repo_name, "master")

    if commit_arg == "commit_obj":
        client.finish_commit(commit)
    elif commit_arg == "(repo, commit_id)":
        client.finish_commit((repo_name, commit.id))
    elif commit_arg == "(repo, branch)":
        client.finish_commit((repo_name, "master"))
    elif commit_arg == "(repo, branch, commit_id)":
        client.finish_commit((repo_name, "master", commit.id))
    elif commit_arg == "(repo, branch, commit_id, type)":
        client.finish_commit((repo_name, "master", commit.id, "user"))
    elif commit_arg == "dictionary":
        client.finish_commit({"repo": repo_name, "id": commit.id, "branch": "master"})

    client.wait_commit(commit)
    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == commit.id

    commit_match_count = len(
        [
            c
            for c in commit_infos
            if c.commit.id == commit.id and c.finished.seconds != 0
        ]
    )
    assert commit_match_count == 1
    assert commit_infos[0].finished.seconds != 0
    assert commit_infos[0].finished.nanos != 0


def test_commit_context_mgr():
    """Start and finish a commit using a context manager."""

    client, repo_name = sandbox("commit_context_mgr")

    with client.commit(repo_name, "master") as c1:
        pass
    with client.commit(repo_name, "master") as c2:
        pass

    with pytest.raises(python_pachyderm.RpcError):
        with client.commit("some-fake-repo", "master"):
            pass

    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 2
    assert sorted([c.commit.id for c in commit_infos]) == sorted([c1.id, c2.id])


def test_put_file_bytes_bytestring():
    """
    Start and finish a commit using a context manager while putting a file
    from a bytesting.
    """

    client, repo_name = sandbox("put_file_bytes_bytestring")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "file.dat", b"DATA")

    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    files = list(client.list_file((repo_name, c.id), ""))
    assert len(files) == 1


def test_put_file_bytes_filelike():
    """
    Start and finish a commit using a context manager while putting a file
    from a file-like object.
    """

    client, repo_name = sandbox("put_file_bytes_filelike")

    with client.commit(repo_name, "master") as commit:
        client.put_file_bytes(commit, "file.dat", BytesIO(b"DATA"))

    files = list(client.list_file(commit, ""))
    assert len(files) == 1


def test_put_file_zero_bytes_mfc():
    """
    Put a zero-byte file using PutFileClient
    """

    client, repo_name = sandbox("put_file_bytes_file_zero_byte")

    commit = (repo_name, "master")
    with client.modify_file_client(commit) as c:
        c.put_file_from_bytes("file.dat", b"")
    files = list(client.list_file(commit, "/"))
    assert len(files) == 1
    fi = client.inspect_file(commit, "file.dat")
    assert fi.size_bytes == 0


def test_put_file_bytes_zero_bytes_direct():
    """
    Put a zero-byte file using a bytestring
    """

    client, repo_name = sandbox("put_file_bytes_zero_bytes")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "empty_bytestring.dat", b"")
    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    fi = client.inspect_file(c, "empty_bytestring.dat")
    assert fi.size_bytes == 0


def test_put_file_bytes_large():
    """
    Put a file larger than the maximum message size.
    """

    client, repo_name = sandbox("put_file_bytes_large")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "file.dat", b"#" * (21 * 1024 * 1024))

    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    files = list(client.list_file(c, ""))
    assert len(files) == 1


def test_put_file_url():
    client, repo_name = sandbox("put_file_url")

    with client.commit(repo_name, "master") as c:
        client.put_file_url(
            c,
            "index.html",
            "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html",
        )

    files = list(client.list_file(c, ""))
    assert len(files) == 1
    assert files[0].file.path == "/index.html"


def test_put_file_empty():
    client, repo_name = sandbox("put_file_empty")
    commit = (repo_name, "master")

    with tempfile.NamedTemporaryFile() as f:
        with client.modify_file_client(commit) as mfc:
            mfc.put_file_from_fileobj("file1.dat", BytesIO(b""))
            mfc.put_file_from_url(
                "index.html",
                "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html",
            )
            mfc.put_file_from_bytes("file2.dat", b"DATA2")

            f.write(b"DATA3")
            f.flush()
            mfc.put_file_from_filepath("file3.dat", f.name)

    files = list(client.list_file(commit, ""))
    # assert len(files) == 4
    assert files[0].file.path == "/file1.dat"
    assert files[1].file.path == "/file2.dat"
    assert files[2].file.path == "/file3.dat"
    assert files[3].file.path == "/index.html"


def test_put_file_atomic():
    client, repo_name = sandbox("put_file_atomic")
    commit = (repo_name, "master")

    with tempfile.NamedTemporaryFile() as f:
        with client.modify_file_client(commit) as mfc:
            mfc.put_file_from_fileobj("file1.dat", BytesIO(b"DATA1"))
            mfc.put_file_from_bytes("file2.dat", b"DATA2")
            mfc.put_file_from_url(
                "index.html",
                "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html",
            )

            f.write(b"DATA3")
            f.flush()
            mfc.put_file_from_filepath("file3.dat", f.name)

    files = list(client.list_file(commit, ""))
    assert len(files) == 4
    assert files[0].file.path == "/file1.dat"
    assert files[1].file.path == "/file2.dat"
    assert files[2].file.path == "/file3.dat"
    assert files[3].file.path == "/index.html"

    with client.modify_file_client(commit) as mfc:
        mfc.delete_file("/file1.dat")
        mfc.delete_file("/file2.dat")
        mfc.delete_file("/file3.dat")

    files = list(client.list_file(commit, ""))
    assert len(files) == 1
    assert files[0].file.path == "/index.html"


def test_copy_file():
    client, repo_name = sandbox("copy_file")

    with client.commit(repo_name, "master") as src_commit:
        client.put_file_bytes(src_commit, "file1.dat", BytesIO(b"DATA1"))
        client.put_file_bytes(src_commit, "file2.dat", BytesIO(b"DATA2"))

    with client.commit(repo_name, "master") as dest_commit:
        client.copy_file(src_commit, "file1.dat", dest_commit, "copy.dat")

    files = list(client.list_file(dest_commit, ""))
    assert len(files) == 3
    assert files[0].file.path == "/copy.dat"
    assert files[1].file.path == "/file1.dat"
    assert files[2].file.path == "/file2.dat"


def test_wait_commit():
    """
    Ensure wait_commit works
    """
    client, repo_name = sandbox("wait_commit")
    repo2_name = util.create_test_repo(client, "wait_commit2")

    # Create provenance between repos (which creates a new commit)
    client.create_branch(
        repo2_name,
        "master",
        provenance=[
            pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name="master"
            )
        ],
    )
    # Head commit is still open in repo2
    client.finish_commit((repo2_name, "master"))

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "input.json", b"hello world")
    client.finish_commit((repo2_name, "master"))

    # Just block until all of the commits are yielded
    commits = client.wait_commit(c.id)
    assert len(commits) == 2
    assert commits[1].finished

    with client.commit(repo_name, "master") as c2:
        client.put_file_bytes(c2, "input.json", b"bye world")
    client.finish_commit((repo2_name, "master"))

    # Just block until the commit in repo1 is finished
    commits = client.wait_commit(c2)
    assert len(commits) == 1
    assert commits[0].finished

    files = list(client.list_file(c2, "/"))
    assert len(files) == 1


def test_inspect_commit():
    client, repo_name = sandbox("inspect_commit")
    repo2_name = util.create_test_repo(client, "inspect_commit2")

    # Create provenance between repos (which creates a new commit)
    client.create_branch(
        repo2_name,
        "master",
        provenance=[
            pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name="master"
            )
        ],
    )
    # Head commit is still open in repo2
    client.finish_commit((repo2_name, "master"))

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "input.json", b"hello world")
    client.finish_commit((repo2_name, "master"))

    # Inspect commit at a specific repo
    commits = list(client.inspect_commit(c, pfs_proto.CommitState.FINISHED))
    assert len(commits) == 1

    commit = commits[0]
    assert commit.commit.branch.name == "master"
    assert commit.finished
    assert commit.description == ""
    # assert commit.size_bytes == 11
    assert len(commit.commit.id) == 32
    assert commit.commit.branch.repo.name == repo_name

    # Inspect entire commit
    commits = list(client.inspect_commit(c.id, pfs_proto.CommitState.FINISHED))
    assert len(commits) == 2


def test_squash_commit():
    client, repo_name = sandbox("squash_commit")
    repo2_name = util.create_test_repo(client, "squash_commit2")

    # Create provenance between repos (which creates an auto commit)
    client.create_branch(
        repo2_name,
        "master",
        provenance=[
            pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name="master"
            )
        ],
    )
    # Head commit is still open in repo2
    client.finish_commit((repo2_name, "master"))

    with client.commit(repo_name, "master") as commit1:
        pass
    client.finish_commit((repo2_name, "master"))

    with client.commit(repo_name, "master") as commit2:
        pass
    client.finish_commit((repo2_name, "master"))

    client.wait_commit(commit2.id)

    commits = list(client.list_commit(repo_name))
    assert len(commits) == 2

    client.squash_commit(commit1.id)
    commits = list(client.list_commit(repo_name))
    assert len(commits) == 1

    commits = list(client.list_commit(repo2_name))
    assert len(commits) == 0  # since list_commit defaults to user commits
    commits = list(
        client.list_commit(repo2_name, origin_kind=pfs_proto.OriginKind.AUTO)
    )
    assert len(commits) == 2


def test_drop_commit():
    client, repo_name = sandbox("drop_commit")
    repo2_name = util.create_test_repo(client, "drop_commit2")

    # Create provenance between repos (which creates an auto commit)
    client.create_branch(
        repo2_name,
        "master",
        provenance=[
            pfs_proto.Branch(
                repo=pfs_proto.Repo(name=repo_name, type="user"), name="master"
            )
        ],
    )
    # Head commit is still open in repo2
    client.finish_commit((repo2_name, "master"))

    with client.commit(repo_name, "master"):
        pass
    client.finish_commit((repo2_name, "master"))

    with client.commit(repo_name, "master") as commit2:
        pass
    client.finish_commit((repo2_name, "master"))

    client.wait_commit(commit2.id)

    commits = list(client.list_commit(repo_name))
    assert len(commits) == 2

    client.drop_commit(commit2.id)
    commits = list(client.list_commit(repo_name))
    assert len(commits) == 1

    commits = list(client.list_commit(repo2_name))
    assert len(commits) == 0  # since list_commit defaults to user commits
    commits = list(
        client.list_commit(repo2_name, origin_kind=pfs_proto.OriginKind.AUTO)
    )
    assert len(commits) == 2


def test_subscribe_commit():
    client, repo_name = sandbox("subscribe_commit")
    commits = client.subscribe_commit(repo_name, "master")

    with client.commit(repo_name, "master"):
        pass

    commit = next(commits)
    assert commit.commit.branch.repo.name == repo_name
    assert commit.commit.branch.name == "master"


def test_list_commit():
    python_pachyderm.Client().delete_all_repos()

    client, repo_name1 = sandbox("list_commit1")

    with client.commit(repo_name1, "master"):
        pass
    with client.commit(repo_name1, "master"):
        pass

    repo_name2 = util.create_test_repo(client, "list_commit2")

    with client.commit(repo_name2, "master"):
        pass

    commits = list(client.list_commit())
    assert len(commits) == 3


def test_list_branch():
    client, repo_name = sandbox("list_branch")

    with client.commit(repo_name, "master"):
        pass
    with client.commit(repo_name, "develop"):
        pass

    branches = list(client.list_branch(repo_name))
    assert len(branches) == 2
    assert branches[0].branch.name == "develop"
    assert branches[1].branch.name == "master"


def test_delete_branch():
    client, repo_name = sandbox("delete_branch")

    with client.commit(repo_name, "develop"):
        pass

    branches = list(client.list_branch(repo_name))
    assert len(branches) == 1
    client.delete_branch(repo_name, "develop")
    branches = list(client.list_branch(repo_name))
    assert len(branches) == 0


def test_inspect_file():
    client, repo_name = sandbox("inspect_file")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "file.dat", b"DATA")

    fi = client.inspect_file(c, "file.dat")
    assert fi.file.commit.id == c.id
    assert fi.file.commit.branch.repo.name == repo_name
    assert fi.file.path == "/file.dat"
    # assert fi.size_bytes == 4


def test_list_file():
    client, repo_name = sandbox("list_file")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "file1.dat", b"DATA")
        client.put_file_bytes(c, "file2.dat", b"DATA")

    files = list(client.list_file((repo_name, c.id), "/"))
    assert len(files) == 2
    # assert files[0].size_bytes == 4
    assert files[0].file_type == pfs_proto.FileType.FILE
    assert files[0].file.path == "/file1.dat"
    # assert files[1].size_bytes == 4
    assert files[1].file_type == pfs_proto.FileType.FILE
    assert files[1].file.path == "/file2.dat"


def test_walk_file():
    client, repo_name = sandbox("walk_file")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "/file1.dat", b"DATA")
        client.put_file_bytes(c, "/a/file2.dat", b"DATA")
        client.put_file_bytes(c, "/a/b/file3.dat", b"DATA")

    files = list(client.walk_file(c, "/a"))
    assert len(files) == 4
    assert files[0].file.path == "/a/"
    assert files[1].file.path == "/a/b/"
    assert files[2].file.path == "/a/b/file3.dat"
    assert files[3].file.path == "/a/file2.dat"


def test_glob_file():
    client, repo_name = sandbox("glob_file")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "file1.dat", b"DATA")
        client.put_file_bytes(c, "file2.dat", b"DATA")

    files = list(client.glob_file(c, "/*.dat"))
    assert len(files) == 2
    # assert files[0].size_bytes == 4
    assert files[0].file_type == pfs_proto.FileType.FILE
    assert files[0].file.path == "/file1.dat"
    # assert files[1].size_bytes == 4
    assert files[1].file_type == pfs_proto.FileType.FILE
    assert files[1].file.path == "/file2.dat"

    files = list(client.glob_file(c, "/*1.dat"))
    assert len(files) == 1
    # assert files[0].size_bytes == 4
    assert files[0].file_type == pfs_proto.FileType.FILE
    assert files[0].file.path == "/file1.dat"


def test_delete_file():
    client, repo_name = sandbox("delete_file")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "file1.dat", b"DATA")

    assert len(list(client.list_file(c, "/"))) == 1

    with client.commit(repo_name, "master") as c:
        client.delete_file(c, "file1.dat")

    assert len(list(client.list_file(c, "/"))) == 0


def test_create_branch():
    client, repo_name = sandbox("create_branch")
    client.create_branch(repo_name, "foobar")
    branches = list(client.list_branch(repo_name))
    assert len(branches) == 1
    assert branches[0].branch.name == "foobar"


def test_inspect_branch():
    client, repo_name = sandbox("inspect_branch")
    client.create_branch(repo_name, "foobar")
    branch = client.inspect_branch(repo_name, "foobar")
    assert branch.branch.name == "foobar"


def test_fsck():
    client = python_pachyderm.Client()
    assert len(list(client.fsck())) == 0


def test_diff_file():
    client, repo_name = sandbox("diff_file")

    with client.commit(repo_name, "master") as old_commit:
        client.put_file_bytes(old_commit, "file1.dat", BytesIO(b"old data 1"))
        client.put_file_bytes(old_commit, "file2.dat", BytesIO(b"old data 2"))

    with client.commit(repo_name, "master") as new_commit:
        client.put_file_bytes(new_commit, "file1.dat", BytesIO(b"new data 1"))

    diff = list(client.diff_file(new_commit, "file1.dat", old_commit, "file2.dat"))
    assert diff[0].new_file.file.path == "/file1.dat"
    assert diff[1].old_file.file.path == "/file2.dat"


def test_path_exists():
    client, repo_name = sandbox("path_exists")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "dir/file1", b"I'm a file in a dir.")
        client.put_file_bytes(c, "file2", b"I'm a file.")

    assert client.path_exists(c, "/")
    assert client.path_exists(c, "dir/")
    assert client.path_exists(c, "dir")
    assert client.path_exists(c, "dir/file1")
    assert client.path_exists(c, "dir/file1/")
    assert client.path_exists(c, "file2")
    assert not client.path_exists(c, "file1")

    with pytest.raises(ValueError, match=r"nonexistent commit provided"):
        assert not client.path_exists(("fake_repo", "master"), "dir")


def test_modify_file_client():
    client, repo_name = sandbox("modify_file_client")

    # test on open commit
    c = client.start_commit(repo_name, "master")
    with client.modify_file_client(c) as mfc:
        mfc.put_file_from_bytes("/file1.txt", b"DATA1")
        mfc.put_file_from_fileobj("/file2.txt", BytesIO(b"DATA2"))

    client.finish_commit(c)
    client.wait_commit(c)

    assert len(list(client.list_commit(repo_name))) == 1
    assert len(list(client.list_file(c, "/"))) == 2

    # test on unopened commit
    c2 = (repo_name, "master")
    with client.modify_file_client(c2) as mfc:
        mfc.delete_file("/file2.txt")
        mfc.copy_file(c, "/file1.txt", "/file3.txt")

    client.wait_commit(c2)

    assert len(list(client.list_commit(repo_name))) == 2

    files = list(client.list_file(c2, "/"))
    assert len(files) == 2
    assert "/file3.txt" in [f.file.path for f in files]


@pytest.fixture(name="repo")
def _repo_fixture(request) -> str:
    """Create a repository name from the test function name."""
    return request.node.nodeid.replace("/", "-").replace(":", "-").replace(".py", "")


@pytest.fixture(name="client")
def _client_fixture(repo):
    client = Client()
    client.delete_repo(repo, force=True)
    client.create_repo(repo, "test repo for python_pachyderm")
    yield client
    client.delete_repo(repo, force=True)


class TestPFSFile:
    @staticmethod
    def test_get_large_file(client: Client, repo: str):
        """Test that a large file (requires >1 gRPC message to stream)
        is successfully streamed in it's entirety.
        """
        # Arrange
        data = os.urandom(int(MAX_RECEIVE_MESSAGE_SIZE * 1.1))
        pfs_file = "/large_file.dat"

        with client.commit(repo, "large_file_commit") as commit:
            client.put_file_bytes(commit, pfs_file, data)

        # Act
        with client.get_file(commit, pfs_file) as file:
            assert len(file._buffer) < len(data), (
                "PFSFile initialization streams the first message. "
                "This asserts that the test file must be streamed over multiple messages. "
            )
            streamed_data = file.read()

        # Assert
        assert streamed_data[:10] == data[:10]
        assert streamed_data[-10:] == data[-10:]
        assert len(streamed_data) == len(data)

    @staticmethod
    def test_buffered_data(client: Client, repo: str):
        """Test that data gets buffered as expected."""
        # Arrange
        BytesValue = NamedTuple("BytesValue", [("value", bytes)])
        stream_items = [b"a", b"bc", b"def", b"ghij", b"klmno"]
        stream = (BytesValue(value=item) for item in stream_items)

        # Act
        file = PFSFile(stream)

        # Assert
        assert file._buffer == b"a"
        assert file.read(0) == b""
        assert file._buffer == b"a"
        assert file.read(2) == b"ab"
        assert file._buffer == b"c"
        assert file.read(5) == b"cdefg"
        assert file._buffer == b"hij"

    @staticmethod
    def test_fail_early(client: Client, repo: str):
        """Test that gRPC errors are caught and thrown early."""
        # Arrange
        commit = (repo, "master")

        # Act & Assert
        with pytest.raises(ConnectionError):
            client.get_file(commit, "there is no file")

    @staticmethod
    def test_context_manager(client: Client, repo: str):
        """Test that the PFSFile context manager cleans up as expected.

        Note: The test file needs to be large in order to span multiple
          gRPC messages. If the file only requires a single message to stream
          then the stream will be terminated and the assertion within the
          `client.get_file` context will fail.
          Maybe this should be rethought or mocked in the future.
        """
        # Arrange
        data = os.urandom(int(MAX_RECEIVE_MESSAGE_SIZE * 1.1))
        pfs_file = "/test_file.dat"

        with client.commit(repo, "master") as commit:
            client.put_file_bytes(commit, pfs_file, data)

        # Act & Assert
        with client.get_file(commit, pfs_file) as file:
            assert file._stream.is_active()
        assert not file._stream.is_active()

    @staticmethod
    def test_cancelled_stream(client: Client, repo: str):
        """Test that a cancelled stream maintains the integrity of the
        already-streamed data.
        """
        # Arrange
        data = os.urandom(200)
        pfs_file = "/test_file.dat"

        with client.commit(repo, "master") as commit:
            client.put_file_bytes(commit, pfs_file, data)

        # Act & Assert
        with client.get_file(commit, pfs_file) as file:
            assert file.read(100) == data[:100]
        assert file.read(100) == data[100:]

    @staticmethod
    def test_get_file_tar(client: Client, repo: str, tmp_path: Path):
        """Test that retrieving a TAR of a PFS directory works as expected."""
        # Arrange
        TestPfsFile = NamedTuple("TestPfsFile", (("data", bytes), ("path", str)))
        test_files = [
            TestPfsFile(os.urandom(1024), "/a.dat"),
            TestPfsFile(os.urandom(2048), "/child/b.dat"),
            TestPfsFile(os.urandom(5000), "/child/grandchild/c.dat"),
        ]

        with client.commit(repo, "master") as commit:
            for test_file in test_files:
                client.put_file_bytes(commit, test_file.path, test_file.data)

        # Act
        with client.get_file_tar(commit, "/") as tar:
            tar.extractall(tmp_path)

        # Assert
        for test_file in test_files:
            local_file = tmp_path.joinpath(test_file.path[1:])
            assert local_file.exists()
            assert local_file.read_bytes() == test_file.data
