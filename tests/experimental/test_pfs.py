#!/usr/bin/env python

"""Tests PFS-related functionality"""
import os
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import grpclib
import pytest
from betterproto import BytesValue

from python_pachyderm import PFSFile
from python_pachyderm.service import MAX_RECEIVE_MESSAGE_SIZE
from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.pfs import CommitState, FileType

REMOTE_CONTENT_URL = "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html"


def test_inspect_repo(client: ExperimentalClient, repo: str):
    client.pfs.inspect_repo(repo)
    assert any(info for info in client.pfs.list_repo() if info.repo.name == repo)


def test_delete_repo(client: ExperimentalClient, repo: str):
    assert any(info for info in client.pfs.list_repo() if info.repo.name == repo)
    client.pfs.delete_repo(repo)
    assert all(info for info in client.pfs.list_repo() if info.repo.name != repo)


def test_delete_non_existent_repo(client: ExperimentalClient):
    original_count = len(list(client.pfs.list_repo()))
    client.pfs.delete_repo("BOGUS_NAME")
    assert original_count == len(list(client.pfs.list_repo()))


def test_delete_all_repos(client: ExperimentalClient, repo: str):
    assert len(list(client.pfs.list_repo())) > 0
    client.pfs.delete_all_repos()
    assert len(list(client.pfs.list_repo())) == 0


def test_start_commit(client: ExperimentalClient, repo: str):
    commit = client.pfs.start_commit(repo, "master")
    assert commit.branch.repo.name == repo

    # cannot start new commit before the previous one is finished
    error_match = r"parent commit .* has not been finished"
    with pytest.raises(grpclib.GRPCError, match=error_match):
        client.pfs.start_commit(repo, "master")

    client.pfs.finish_commit(commit)
    commit2 = client.pfs.start_commit(repo, "master")
    assert commit2.branch.repo.name == repo

    with pytest.raises(grpclib.GRPCError, match=r"repo .* not found"):
        client.pfs.start_commit("some-fake-repo", "master")


def test_start_commit_on_branch_with_parent(client: ExperimentalClient, repo: str):
    commit1 = client.pfs.start_commit(repo, branch="master")
    client.pfs.finish_commit(commit1)

    commit2 = client.pfs.start_commit(repo, branch="master", parent=commit1.id)
    assert commit2.branch.repo.name == repo


def test_start_commit_fork(client: ExperimentalClient, repo: str):
    commit1 = client.pfs.start_commit(repo, branch="master")
    client.pfs.finish_commit(commit1)

    commit2 = client.pfs.start_commit(repo, branch="patch", parent="master")
    assert commit2.branch.repo.name == repo

    branches = [info.branch.name for info in client.pfs.list_branch(repo)]
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
def test_finish_commit(commit_arg, client: ExperimentalClient, repo: str):
    commit = client.pfs.start_commit(repo, "master")

    if commit_arg == "commit_obj":
        client.pfs.finish_commit(commit)
    elif commit_arg == "(repo, commit_id)":
        client.pfs.finish_commit((repo, commit.id))
    elif commit_arg == "(repo, branch)":
        client.pfs.finish_commit((repo, "master"))
    elif commit_arg == "(repo, branch, commit_id)":
        client.pfs.finish_commit((repo, "master", commit.id))
    elif commit_arg == "(repo, branch, commit_id, type)":
        client.pfs.finish_commit((repo, "master", commit.id, "user"))
    elif commit_arg == "dictionary":
        client.pfs.finish_commit(dict(repo=repo, id=commit.id, branch="master"))

    client.pfs.wait_commit(commit)
    commit_infos = list(client.pfs.list_commit(repo))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == commit.id

    commit_match_count = len(
        [c for c in commit_infos if c.commit.id == commit.id and c.finished]
    )
    assert commit_match_count == 1
    assert commit_infos[0].finished > commit_infos[0].started
    assert commit_infos[0].finished > commit_infos[0].finishing


def test_commit_context_mgr(client: ExperimentalClient, repo: str):
    """Start and finish a commit using a context manager."""

    with client.pfs.commit(repo, "master") as c1:
        pass
    with client.pfs.commit(repo, "master") as c2:
        pass

    with pytest.raises(grpclib.GRPCError):
        with client.pfs.commit("some-fake-repo", "master"):
            pass

    commit_infos = list(client.pfs.list_commit(repo))
    assert len(commit_infos) == 2
    assert sorted([c.commit.id for c in commit_infos]) == sorted([c1.id, c2.id])


def test_put_file_bytes_bytestring(client: ExperimentalClient, repo: str):
    """
    Start and finish a commit using a context manager while putting a file
    from a bytesting.
    """
    file_path = "/file.dat"
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, file_path, b"DATA")

    assert any(
        info for info in client.pfs.list_commit(repo) if info.commit.id == commit.id
    )
    assert any(
        info
        for info in client.pfs.list_file((repo, commit.id), "/")
        if info.file.path == file_path
    )


def test_put_file_bytes_filelike(client: ExperimentalClient, repo: str):
    """
    Start and finish a commit using a context manager while putting a file
    from a file-like object.
    """
    file_path = "/file.dat"
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, file_path, BytesIO(b"DATA"))

    assert any(
        info
        for info in client.pfs.list_file((repo, commit.id), "/")
        if info.file.path == file_path
    )


def test_put_file_zero_bytes_mfc(client: ExperimentalClient, repo: str):
    """
    Put a zero-byte file using PutFileClient
    """
    file_path = "/file.dat"
    with client.pfs.modify_file_client((repo, "master")) as mfc:
        mfc.put_file_from_bytes(file_path, b"")
        commit = mfc.commit

    assert any(
        info
        for info in client.pfs.list_file(commit, "/")
        if info.file.path == file_path
    )
    assert client.pfs.inspect_file(commit, file_path).size_bytes == 0


def test_put_file_bytes_zero_bytes_direct(client: ExperimentalClient, repo: str):
    """
    Put a zero-byte file using a bytestring
    """
    file_path = "/empty_bytestring.dat"
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, file_path, b"")

    assert any(
        info for info in client.pfs.list_commit(repo) if info.commit.id == commit.id
    )
    assert client.pfs.inspect_file(commit, file_path).size_bytes == 0


def test_put_file_url(client: ExperimentalClient, repo: str):
    file_path = "/index.html"
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_url(commit, file_path, REMOTE_CONTENT_URL)

    assert any(
        info
        for info in client.pfs.list_file(commit, "/")
        if info.file.path == file_path
    )


def test_put_file_empty(client: ExperimentalClient, repo: str, tmp_path: Path):
    file_path = tmp_path / "file3.dat"
    file_path.write_bytes(b"DATA3")
    with client.pfs.modify_file_client((repo, "master")) as mfc:
        mfc.put_file_from_fileobj("/file1.dat", BytesIO(b""))
        mfc.put_file_from_url("/index.html", REMOTE_CONTENT_URL)
        mfc.put_file_from_bytes("/file2.dat", b"DATA2")
        mfc.put_file_from_filepath("/file3.dat", file_path)
        commit = mfc.commit

    files = list(client.pfs.list_file(commit, "/"))
    assert files[0].file.path == "/file1.dat"
    assert files[1].file.path == "/file2.dat"
    assert files[2].file.path == "/file3.dat"
    assert files[3].file.path == "/index.html"

    with client.pfs.modify_file_client(commit) as mfc:
        mfc.delete_file("/file1.dat")
        mfc.delete_file("/file2.dat")
        mfc.delete_file("/file3.dat")

    files = list(client.pfs.list_file(commit, "/"))
    assert len(files) == 1
    assert files[0].file.path == "/index.html"


def test_copy_file(client: ExperimentalClient, repo: str):
    with client.pfs.commit(repo, "master") as src_commit:
        client.pfs.put_file_bytes(src_commit, "/file1.dat", BytesIO(b"DATA1"))
        client.pfs.put_file_bytes(src_commit, "/file2.dat", BytesIO(b"DATA2"))

    with client.pfs.commit(repo, "master") as dest_commit:
        client.pfs.copy_file(src_commit, "/file1.dat", dest_commit, "/copy.dat")

    files = list(client.pfs.list_file(dest_commit, "/"))
    assert len(files) == 3
    assert files[0].file.path == "/copy.dat"
    assert files[1].file.path == "/file1.dat"
    assert files[2].file.path == "/file2.dat"


def test_inspect_commit(client: ExperimentalClient, repo: str):
    branch = "master"
    description = "test commit"
    with client.pfs.commit(repo, branch, description=description) as commit:
        client.pfs.put_file_bytes(commit, "/input.json", b"hello world")

    # Inspect commit at a specific repo
    commits = list(client.pfs.inspect_commit(commit, CommitState.FINISHED))
    assert len(commits) == 1

    commit_info = commits[0]
    assert commit_info.commit.branch.name == branch
    assert commit_info.finished
    assert commit_info.description == description
    assert commit_info.commit.id == commit.id
    assert commit_info.commit.branch.repo.name == repo


def test_squash_commit(client: ExperimentalClient, repo: str):
    with client.pfs.commit(repo, "master") as commit1:
        pass

    with client.pfs.commit(repo, "master") as commit2:
        pass

    client.pfs.wait_commit(commit2)

    commit_ids = [info.commit.id for info in client.pfs.list_commit(repo)]
    assert commit1.id in commit_ids
    assert commit2.id in commit_ids

    client.pfs.squash_commit(commit1.id)
    commit_ids = [info.commit.id for info in client.pfs.list_commit(repo)]
    assert commit1.id not in commit_ids
    assert commit2.id in commit_ids


def test_drop_commit(client: ExperimentalClient, repo: str):
    with client.pfs.commit(repo, "master") as commit1:
        pass

    with client.pfs.commit(repo, "master") as commit2:
        pass

    client.pfs.wait_commit(commit2)

    commit_ids = [info.commit.id for info in client.pfs.list_commit(repo)]
    assert commit1.id in commit_ids
    assert commit2.id in commit_ids

    client.pfs.drop_commit(commit2.id)
    commit_ids = [info.commit.id for info in client.pfs.list_commit(repo)]
    assert commit1.id in commit_ids
    assert commit2.id not in commit_ids


def test_subscribe_commit(client: ExperimentalClient, repo: str):
    branch = "master"
    subscription = client.pfs.subscribe_commit(repo, branch)

    with client.pfs.commit(repo, branch):
        pass

    commit_info = next(subscription)
    assert commit_info.commit.branch.repo.name == repo
    assert commit_info.commit.branch.name == branch


def test_list_branch(client: ExperimentalClient, repo: str):
    with client.pfs.commit(repo, "master"):
        pass
    with client.pfs.commit(repo, "develop"):
        pass

    branches = [info.branch.name for info in client.pfs.list_branch(repo)]
    assert "develop" in branches
    assert "master" in branches


def test_delete_branch(client: ExperimentalClient, repo: str):
    branch = "develop"
    with client.pfs.commit(repo, branch):
        pass

    branches = [info.branch.name for info in client.pfs.list_branch(repo)]
    assert branch in branches

    client.pfs.delete_branch(repo, branch)

    branches = [info.branch.name for info in client.pfs.list_branch(repo)]
    assert branch not in branches


def test_inspect_file(client: ExperimentalClient, repo: str):
    file_path = "/file.dat"
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, file_path, b"DATA")

    info = client.pfs.inspect_file(commit, file_path)
    assert info.file.commit.id == commit.id
    assert info.file.commit.branch.repo.name == repo
    assert info.file.path == file_path


def test_walk_file(client: ExperimentalClient, repo: str):
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, "/file1.dat", b"DATA")
        client.pfs.put_file_bytes(commit, "/a/file2.dat", b"DATA")
        client.pfs.put_file_bytes(commit, "/a/b/file3.dat", b"DATA")

    files = list(client.pfs.walk_file(commit, "/a"))
    assert len(files) == 4
    assert files[0].file.path == "/a/"
    assert files[1].file.path == "/a/b/"
    assert files[2].file.path == "/a/b/file3.dat"
    assert files[3].file.path == "/a/file2.dat"


def test_glob_file(client: ExperimentalClient, repo: str):
    with client.pfs.commit(repo, "master") as c:
        client.pfs.put_file_bytes(c, "/file1.dat", b"DATA")
        client.pfs.put_file_bytes(c, "/file2.dat", b"DATA")

    files = list(client.pfs.glob_file(c, "/*.dat"))
    assert len(files) == 2
    assert files[0].file_type == FileType.FILE
    assert files[0].file.path == "/file1.dat"
    assert files[1].file_type == FileType.FILE
    assert files[1].file.path == "/file2.dat"

    files = list(client.pfs.glob_file(c, "/*1.dat"))
    assert len(files) == 1
    assert files[0].file_type == FileType.FILE
    assert files[0].file.path == "/file1.dat"


def test_delete_file(client: ExperimentalClient, repo: str):
    file_path = "/file.dat"
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, file_path, b"DATA")

    assert any(
        info
        for info in client.pfs.list_file((repo, commit.id), "/")
        if info.file.path == file_path
    )

    with client.pfs.commit(repo, "master") as commit:
        client.pfs.delete_file(commit, file_path)

    assert all(
        info
        for info in client.pfs.list_file((repo, commit.id), "/")
        if info.file.path != file_path
    )


def test_create_branch(client: ExperimentalClient, repo: str):
    client.pfs.create_branch(repo, "foobar")
    branches = [info.branch.name for info in client.pfs.list_branch(repo)]
    assert "foobar" in branches


def test_inspect_branch(client: ExperimentalClient, repo: str):
    client.pfs.create_branch(repo, "foobar")
    branch = client.pfs.inspect_branch(repo, "foobar")
    assert branch.branch.name == "foobar"


def test_fsck(client: ExperimentalClient):
    assert len(list(client.pfs.fsck())) == 0


def test_diff_file(client: ExperimentalClient, repo: str):
    file1, file2 = "/file1.dat", "/file2.dat"
    with client.pfs.commit(repo, "master") as old_commit:
        client.pfs.put_file_bytes(old_commit, file1, BytesIO(b"old data 1"))
        client.pfs.put_file_bytes(old_commit, file2, BytesIO(b"old data 2"))

    with client.pfs.commit(repo, "master") as new_commit:
        client.pfs.put_file_bytes(new_commit, file1, BytesIO(b"new data 1"))

    diff = list(client.pfs.diff_file(new_commit, file1, old_commit, file2))
    assert diff[0].new_file.file.path == file1
    assert diff[1].old_file.file.path == file2


def test_path_exists(client: ExperimentalClient, repo: str):
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, "/dir/file1", b"I'm a file in a dir.")
        client.pfs.put_file_bytes(commit, "/file2", b"I'm a file.")

    assert client.pfs.path_exists(commit, "/")
    assert client.pfs.path_exists(commit, "/dir/")
    assert client.pfs.path_exists(commit, "/dir")
    assert client.pfs.path_exists(commit, "/dir/file1")
    assert client.pfs.path_exists(commit, "/dir/file1/")
    assert client.pfs.path_exists(commit, "/file2")
    assert not client.pfs.path_exists(commit, "/file1")

    with pytest.raises(ValueError, match=r"nonexistent commit provided"):
        assert not client.pfs.path_exists(("fake_repo", "master"), "dir")


def test_mount(client: ExperimentalClient, repo: str, repo2: str, tmp_path: Path):
    file1, file2, file3 = "file1.txt", "file2.txt", "file3.txt"

    with client.pfs.commit(repo, "master") as commit1:
        client.pfs.put_file_bytes(commit1, file1, b"DATA1")
    with client.pfs.commit(repo2, "master") as commit2:
        client.pfs.put_file_bytes(commit2, file2, b"DATA2")

    # Mount all repos
    mount_a = tmp_path / "mount_a"
    client.pfs.mount(mount_a)
    assert (mount_a / repo / file1).read_text() == "DATA1"
    assert (mount_a / repo2 / file2).read_text() == "DATA2"

    # Mount one repo
    mount_b = tmp_path / "mount_b"
    client.pfs.mount(mount_b, [repo2])
    assert (mount_b / repo2 / file2).read_text() == "DATA2"

    client.pfs.unmount(mount_a)
    assert not (mount_a / repo2 / file2).exists()
    client.pfs.unmount(mount_b)
    assert not (mount_b / repo2 / file2).exists()

    with client.pfs.commit(repo, "dev") as commit3:
        client.pfs.put_file_bytes(commit3, file3, b"DATA3")

    # Mount one repo
    mount_c = tmp_path / "mount_c"
    client.pfs.mount(mount_c, [f"{repo}@dev"])
    assert (mount_c / repo / file3).read_text() == "DATA3"

    client.pfs.unmount(mount_c)
    assert not (mount_c / repo / file3).exists()

    # Test runtime error
    mount_d = tmp_path / "mount_d"
    mount_d.mkdir()
    (mount_d / "file.txt").touch()
    with pytest.raises(RuntimeError, match="must be empty to mount"):
        client.pfs.mount(mount_d)


class TestPFSFile:
    @staticmethod
    def test_get_large_file(client: ExperimentalClient, repo: str):
        """Test that a large file (requires >1 gRPC message to stream)
        is successfully streamed in it's entirety.
        """
        # Arrange
        data = os.urandom(int(MAX_RECEIVE_MESSAGE_SIZE * 1.1))
        pfs_file = "/large_file.dat"

        with client.pfs.commit(repo, "large_file_commit") as commit:
            client.pfs.put_file_bytes(commit, pfs_file, data)

        # Act
        with client.pfs.get_file(commit, pfs_file) as file:
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
    def test_buffered_data(client: ExperimentalClient, repo: str):
        """Test that data gets buffered as expected."""
        # Arrange
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
    def test_fail_early(client: ExperimentalClient, repo: str):
        """Test that gRPC errors are caught and thrown early."""
        # Arrange
        commit = (repo, "master")

        # Act & Assert
        with pytest.raises(ConnectionError):
            client.pfs.get_file(commit, "there is no file")

    @staticmethod
    def test_context_manager(client: ExperimentalClient, repo: str):
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

        with client.pfs.commit(repo, "master") as commit:
            client.pfs.put_file_bytes(commit, pfs_file, data)

        # Act & Assert
        with client.pfs.get_file(commit, pfs_file) as file:
            assert file.is_active()
        assert not file.is_active()

    @staticmethod
    def test_cancelled_stream(client: ExperimentalClient, repo: str):
        """Test that a cancelled stream maintains the integrity of the
        already-streamed data.
        """
        # Arrange
        data = os.urandom(200)
        pfs_file = "/test_file.dat"

        with client.pfs.commit(repo, "master") as commit:
            client.pfs.put_file_bytes(commit, pfs_file, data)

        # Act & Assert
        with client.pfs.get_file(commit, pfs_file) as file:
            assert file.read(100) == data[:100]
        assert file.read(100) == data[100:]

    @staticmethod
    def test_get_file_tar(client: ExperimentalClient, repo: str, tmp_path: Path):
        """Test that retrieving a TAR of a PFS directory works as expected."""
        # Arrange
        TestPfsFile = NamedTuple("TestPfsFile", (("data", bytes), ("path", str)))
        test_files = [
            TestPfsFile(os.urandom(1024), "/a.dat"),
            TestPfsFile(os.urandom(2048), "/child/b.dat"),
            TestPfsFile(os.urandom(5000), "/child/grandchild/c.dat"),
        ]

        with client.pfs.commit(repo, "master") as commit:
            for test_file in test_files:
                client.pfs.put_file_bytes(commit, test_file.path, test_file.data)

        # Act
        with client.pfs.get_file_tar(commit, "/") as tar:
            tar.extractall(tmp_path)

        # Assert
        for test_file in test_files:
            local_file = tmp_path.joinpath(test_file.path[1:])
            assert local_file.exists()
            assert local_file.read_bytes() == test_file.data
