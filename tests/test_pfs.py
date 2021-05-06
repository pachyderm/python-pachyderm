#!/usr/bin/env python

"""Tests PFS-related functionality"""

import pytest
import tempfile
from io import BytesIO

import python_pachyderm
from tests import util


def sandbox(test_name):
    client = python_pachyderm.Client()
    repo_name = util.create_test_repo(client, test_name)
    return client, repo_name


def test_inspect_repo():
    client, repo_name = sandbox("inspect_repo")
    client.inspect_repo(repo_name)
    repos = client.list_repo()
    assert len(repos) >= 1
    assert repo_name in [r.repo.name for r in repos]


def test_delete_repo():
    client, repo_name = sandbox("delete_repo")
    orig_repo_count = len(client.list_repo())
    assert orig_repo_count >= 1
    client.delete_repo(repo_name)
    assert len(client.list_repo()) == orig_repo_count - 1


def test_delete_non_existent_repo():
    client = python_pachyderm.Client()
    orig_repo_count = len(client.list_repo())
    client.delete_repo("BOGUS_NAME")
    assert len(client.list_repo()) == orig_repo_count


def test_delete_all_repos():
    client = python_pachyderm.Client()

    util.create_test_repo(client, "test_delete_all_repos", prefix="extra-1")
    util.create_test_repo(client, "test_delete_all_repos", prefix="extra-2")
    assert len(client.list_repo()) >= 2

    client.delete_all_repos()
    assert len(client.list_repo()) == 0


def test_start_commit():
    client, repo_name = sandbox("start_commit")

    commit = client.start_commit(repo_name, "master")
    assert commit.repo.name == repo_name

    commit = client.start_commit(repo_name, None)
    assert commit.repo.name == repo_name

    with pytest.raises(python_pachyderm.RpcError):
        client.start_commit("some-fake-repo", "master")


def test_start_commit_with_parent_no_branch():
    client, repo_name = sandbox("start_commit_with_parent_no_branch")

    commit1 = client.start_commit(repo_name)
    client.finish_commit((repo_name, commit1.id))

    commit2 = client.start_commit(repo_name, parent=commit1.id)
    assert commit2.repo.name == repo_name


def test_start_commit_on_branch_with_parent():
    client, repo_name = sandbox("start_commit_on_branch_with_parent")

    commit1 = client.start_commit(repo_name, branch="master")
    client.finish_commit((repo_name, commit1.id))

    commit2 = client.start_commit(repo_name, branch="master", parent=commit1.id)
    assert commit2.repo.name == repo_name


def test_start_commit_fork():
    client, repo_name = sandbox("start_commit_fork")

    commit1 = client.start_commit(repo_name, branch="master")
    client.finish_commit((repo_name, commit1.id))

    commit2 = client.start_commit(repo_name, branch="patch", parent="master")

    assert commit2.repo.name == repo_name

    branches = [branch_info.name for branch_info in client.list_branch(repo_name)]
    assert "master" in branches
    assert "patch" in branches


@pytest.mark.parametrize(
    "commit_arg", ["commit_obj", "repo/commit_id", "(repo, commit_id)"]
)
def test_finish_commit(commit_arg):
    client, repo_name = sandbox("finish_commit")
    commit = client.start_commit(repo_name)

    if commit_arg == "commit_obj":
        client.finish_commit(commit)
    elif commit_arg == "repo/commit_id":
        client.finish_commit("{}/{}".format(repo_name, commit.id))
    elif commit_arg == "(repo, commit_id)":
        client.finish_commit((repo_name, commit.id))

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
    assert commit_infos[0].finished.seconds != 0
    assert commit_infos[0].finished.nanos != 0


def test_commit_context_mgr():
    """Start and finish a commit using a context manager."""

    client, repo_name = sandbox("commit_context_mgr")

    with client.commit(repo_name, "master") as c1:
        pass
    with client.commit(repo_name, None) as c2:
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

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file.dat", b"DATA")

    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    files = list(client.list_file("{}/{}".format(repo_name, c.id), "."))
    assert len(files) == 1


def test_put_file_bytes_bytestring_with_overwrite():
    """
    Start and finish a commit using a context manager while putting a file
    from a bytesting.
    """

    client, repo_name = sandbox("put_file_bytes_bytestring_with_overwrite")

    with client.commit(repo_name, "mybranch") as c:
        for i in range(5):
            client.put_file_bytes(c, "file.dat", b"DATA")

    with client.commit(repo_name, "mybranch") as c:
        client.put_file_bytes(c, "file.dat", b"FOO", overwrite_index=2)

    # read the file as an iterator
    file = list(client.get_file("{}/{}".format(repo_name, c.id), "file.dat"))
    assert file == [b"DATA", b"DATA", b"FOO"]

    # read the file as a file-like object
    file = client.get_file("{}/{}".format(repo_name, c.id), "file.dat")
    assert file.read(1) == b"D"
    assert file.read(0) == b""
    assert file.read(5) == b"ATADA"
    assert file.read() == b"TAFOO"
    assert file.read(0) == b""
    file.close()  # should be a no-op
    assert file.read() == b""

    # read the file as a file-like object, but close before reading everything
    file = client.get_file("{}/{}".format(repo_name, c.id), "file.dat")
    assert file.read(1) == b"D"
    file.close()
    assert file.read() == b""


def test_put_file_bytes_filelike():
    """
    Start and finish a commit using a context manager while putting a file
    from a file-like object.
    """

    client, repo_name = sandbox("put_file_bytes_filelike")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file.dat", BytesIO(b"DATA"))

    files = list(client.list_file("{}/{}".format(repo_name, c.id), "."))
    assert len(files) == 1


def test_put_file_zero_bytes_pfc():
    """
    Put a zero-byte file using PutFileClient
    """

    client, repo_name = sandbox("put_file_bytes_file_zero_byte")

    commit = "{}/master".format(repo_name)
    with client.put_file_client() as c:
        c.put_file_from_bytes(commit, "file.dat", b"")
    files = list(client.list_file(commit, "."))
    assert len(files) == 1
    fi = client.inspect_file(commit, "file.dat")
    assert fi.size_bytes == 0


def test_put_file_bytes_zero_bytes_direct():
    """
    Put a zero-byte file using a bytestring iterator (tests zero-byte behavior
    in put_file_from_iterable_reqs)
    """

    client, repo_name = sandbox("put_file_bytes_zero_bytes")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "empty_iter.dat", [])
    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    fi = client.inspect_file(c, "empty_iter.dat")
    assert fi.size_bytes == 0

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "empty_bytestring.dat", [b""])
    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 2
    assert commit_infos[0].commit.id == c.id
    fi = client.inspect_file(c, "empty_bytestring.dat")
    assert fi.size_bytes == 0


def test_put_file_bytes_iterable():
    """
    Start and finish a commit using a context manager while putting a file
    from an iterator of bytes.
    """

    client, repo_name = sandbox("put_file_bytes_iterable")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file.dat", [b"DATA"])

    files = list(client.list_file("{}/{}".format(repo_name, c.id), "."))
    assert len(files) == 1


def test_put_file_bytes_large():
    """
    Put a file larger than the maximum message size.
    """

    client, repo_name = sandbox("put_file_bytes_large")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file.dat", b"#" * (21 * 1024 * 1024))

    commit_infos = list(client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    files = list(client.list_file("{}/{}".format(repo_name, c.id), "."))
    assert len(files) == 1


def test_put_file_url():
    client, repo_name = sandbox("put_file_url")

    with client.commit(repo_name) as c:
        client.put_file_url(
            c,
            "index.html",
            "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html",
        )

    files = list(client.list_file("{}/{}".format(repo_name, c.id), "."))
    assert len(files) == 1
    assert files[0].file.path == "/index.html"


def test_put_file_atomic():
    client, repo_name = sandbox("put_file_atomic")
    commit = (repo_name, "master")

    with tempfile.NamedTemporaryFile() as f:
        with client.put_file_client() as pfc:
            pfc.put_file_from_fileobj(commit, "file1.dat", BytesIO(b"DATA1"))
            pfc.put_file_from_bytes(commit, "file2.dat", b"DATA1")
            pfc.put_file_from_url(
                commit,
                "index.html",
                "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html",
            )

            f.write(b"DATA3")
            f.flush()
            pfc.put_file_from_filepath(commit, "file3.dat", f.name)

    files = list(client.list_file(commit, "."))
    assert len(files) == 4
    assert files[0].file.path == "/file1.dat"
    assert files[1].file.path == "/file2.dat"
    assert files[2].file.path == "/file3.dat"
    assert files[3].file.path == "/index.html"

    # atomic deletes are only supported in 1.11.0 onwards
    if util.test_pachyderm_version() >= (1, 11, 0):
        with client.put_file_client() as pfc:
            pfc.delete_file(commit, "/file1.dat")
            pfc.delete_file(commit, "/file2.dat")
            pfc.delete_file(commit, "/file3.dat")

        files = list(client.list_file(commit, "."))
        assert len(files) == 1
        assert files[0].file.path == "/index.html"


def test_copy_file():
    client, repo_name = sandbox("copy_file")

    with client.commit(repo_name, "master") as src_commit:
        client.put_file_bytes(src_commit, "file1.dat", BytesIO(b"DATA1"))
        client.put_file_bytes(src_commit, "file2.dat", BytesIO(b"DATA2"))

    with client.commit(repo_name, "master") as dest_commit:
        client.copy_file(src_commit, "file1.dat", dest_commit, "copy.dat")
        client.copy_file(
            src_commit, "file2.dat", dest_commit, "copy.dat", overwrite=True
        )

    files = list(client.list_file("{}/{}".format(repo_name, dest_commit.id), "."))
    assert len(files) == 3
    assert files[0].file.path == "/copy.dat"
    assert files[1].file.path == "/file1.dat"
    assert files[2].file.path == "/file2.dat"


def test_flush_commit():
    """
    Ensure flush commit works
    """

    client, repo_name = sandbox("flush_commit")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "input.json", b"hello world")

    # Just block until all of the commits are yielded
    list(client.flush_commit(["{}/{}".format(repo_name, c.id)]))

    files = list(client.list_file("{}/master".format(repo_name), "/"))
    assert len(files) == 1


def test_inspect_commit():
    client, repo_name = sandbox("inspect_commit")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "input.json", b"hello world")

    commit = client.inspect_commit("{}/master".format(repo_name))
    if util.test_pachyderm_version() >= (1, 9, 0):
        assert commit.branch.name == "master"
    assert commit.finished
    assert commit.description == ""
    assert commit.size_bytes == 11
    assert len(commit.commit.id) == 32
    assert commit.commit.repo.name == repo_name


def test_delete_commit():
    client, repo_name = sandbox("delete_commit")

    with client.commit(repo_name, "master") as c:
        pass

    commits = list(client.list_commit(repo_name))
    assert len(commits) == 1
    client.delete_commit("{}/master".format(repo_name))
    commits = list(client.list_commit(repo_name))
    assert len(commits) == 0


def test_subscribe_commit():
    client, repo_name = sandbox("subscribe_commit")
    commits = client.subscribe_commit(repo_name, "master")

    with client.commit(repo_name, "master") as c:
        pass

    commit = next(commits)
    if util.test_pachyderm_version() >= (1, 9, 0):
        assert commit.branch.repo.name == repo_name
        assert commit.branch.name == "master"


def test_list_branch():
    client, repo_name = sandbox("list_branch")

    with client.commit(repo_name, "master") as c:
        pass
    with client.commit(repo_name, "develop") as c:
        pass

    branches = client.list_branch(repo_name)
    assert len(branches) == 2
    assert branches[0].name == "develop"
    assert branches[1].name == "master"


def test_delete_branch():
    client, repo_name = sandbox("delete_branch")

    with client.commit(repo_name, "develop") as c:
        pass

    branches = client.list_branch(repo_name)
    assert len(branches) == 1
    client.delete_branch(repo_name, "develop")
    branches = client.list_branch(repo_name)
    assert len(branches) == 0


def test_inspect_file():
    client, repo_name = sandbox("inspect_file")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file.dat", [b"DATA"])

    fi = client.inspect_file(c, "file.dat")
    assert fi.file.commit.id == c.id
    assert fi.file.commit.repo.name == repo_name
    assert fi.file.path == "file.dat"
    assert fi.size_bytes == 4
    assert (
        fi.objects[0].hash
        == "4ba7d4149c32f5ccc6e54190beef0f503d1e637249baa9e4b123f5aa5c89506f299c10a7e32ab1e4bae30ed32df848f87d9b03a640320b0ca758c5ee56cb2db4"
    )


def test_list_file():
    client, repo_name = sandbox("list_file")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file1.dat", [b"DATA"])
        client.put_file_bytes(c, "file2.dat", [b"DATA"])

    files = list(client.list_file(c, "/"))
    assert len(files) == 2
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FileType.FILE.value
    assert files[0].file.path == "/file1.dat"
    assert files[1].size_bytes == 4
    assert files[1].file_type == python_pachyderm.FileType.FILE.value
    assert files[1].file.path == "/file2.dat"


def test_walk_file():
    client, repo_name = sandbox("walk_file")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "/file1.dat", [b"DATA"])
        client.put_file_bytes(c, "/a/file2.dat", [b"DATA"])
        client.put_file_bytes(c, "/a/b/file3.dat", [b"DATA"])

    files = list(client.walk_file(c, "/a"))
    assert len(files) == 4
    assert files[0].file.path == "/a"
    assert files[1].file.path == "/a/b"
    assert files[2].file.path == "/a/b/file3.dat"
    assert files[3].file.path == "/a/file2.dat"


def test_glob_file():
    client, repo_name = sandbox("glob_file")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file1.dat", [b"DATA"])
        client.put_file_bytes(c, "file2.dat", [b"DATA"])

    files = list(client.glob_file(c, "/*.dat"))
    assert len(files) == 2
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FileType.FILE.value
    assert files[0].file.path == "/file1.dat"
    assert files[1].size_bytes == 4
    assert files[1].file_type == python_pachyderm.FileType.FILE.value
    assert files[1].file.path == "/file2.dat"

    files = list(client.glob_file(c, "/*1.dat"))
    assert len(files) == 1
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FileType.FILE.value
    assert files[0].file.path == "/file1.dat"


def test_delete_file():
    client, repo_name = sandbox("delete_file")

    with client.commit(repo_name) as c:
        client.put_file_bytes(c, "file1.dat", [b"DATA"])

    assert len(list(client.list_file(c, "/"))) == 1

    with client.commit(repo_name) as c:
        client.delete_file(c, "file1.dat")

    assert len(list(client.list_file(c, "/"))) == 0


def test_create_branch():
    client, repo_name = sandbox("create_branch")
    client.create_branch(repo_name, "foobar")
    branches = client.list_branch(repo_name)
    assert len(branches) == 1
    assert branches[0].name == "foobar"


def test_inspect_branch():
    client, repo_name = sandbox("inspect_branch")
    client.create_branch(repo_name, "foobar")
    branch = client.inspect_branch(repo_name, "foobar")
    assert branch.branch.name == "foobar"


@util.skip_if_below_pachyderm_version(1, 9, 7)
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

    diff = client.diff_file(new_commit, "file1.dat", old_commit, "file2.dat")
    assert diff.new_files[0].file.path == "file1.dat"
    assert diff.old_files[0].file.path == "file2.dat"

    diff = client.diff_file(new_commit, "file1.dat")
    assert diff.new_files[0].file.path == "file1.dat"
    assert diff.old_files[0].file.path == "file1.dat"
