#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `PfsClient` class of the `python_pachyderm` package."""

import pytest
import random
import string
import threading
from io import BytesIO
from collections import namedtuple

import python_pachyderm

def create_repo(client, test_name):
    repo_name_suffix = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(6))
    repo_name = "{}-{}".format(test_name, repo_name_suffix)
    client.create_repo(repo_name, "repo for {}".format(test_name))
    return repo_name

def sandbox(test_name):
    client = python_pachyderm.PfsClient()
    repo_name = create_repo(client, test_name)
    return client, repo_name

def test_client_init_with_default_host_port():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created without specifying a host or port
    client = python_pachyderm.PfsClient()
    # THEN the GRPC channel should reflect the default of localhost and port 30650
    assert client.channel._channel.target() == b'localhost:30650'


def test_client_init_with_env_vars(monkeypatch):
    # GIVEN a Pachyderm deployment
    # WHEN environment variables are set for Pachyderm host and port
    monkeypatch.setenv('PACHD_ADDRESS', 'pachd.example.com:12345')
    #   AND a client is created without specifying a host or port
    client = python_pachyderm.PfsClient()
    # THEN the GRPC channel should reflect the host and port specified in the environment variables
    assert client.channel._channel.target() == b'pachd.example.com:12345'


def test_client_init_with_args():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created with host and port arguments
    client = python_pachyderm.PfsClient(host='pachd.example.com', port=54321)
    # THEN the GRPC channel should reflect the host and port specified in the arguments
    assert client.channel._channel.target() == b'pachd.example.com:54321'


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
    pfs_client = python_pachyderm.PfsClient()
    orig_repo_count = len(pfs_client.list_repo())
    pfs_client.delete_repo('BOGUS_NAME')
    assert len(pfs_client.list_repo()) == orig_repo_count


def test_delete_all_repos():
    pfs_client = python_pachyderm.PfsClient()

    create_repo(pfs_client, "delete_all_1")
    create_repo(pfs_client, "delete_all_2")
    assert len(pfs_client.list_repo()) >= 2

    pfs_client.delete_all_repos()
    assert len(pfs_client.list_repo()) == 0

def test_start_commit():
    pfs_client, repo_name = sandbox("start_commit")

    commit = pfs_client.start_commit(repo_name, "master")
    assert commit.repo.name == repo_name

    commit = pfs_client.start_commit(repo_name, None)
    assert commit.repo.name == repo_name

    with pytest.raises(Exception):
        pfs_client.start_commit("some-fake-repo", "master")

def test_start_commit_with_parent_no_branch():
    pfs_client, repo_name = sandbox("start_commit_with_parent_no_branch")

    commit1 = pfs_client.start_commit(repo_name)
    pfs_client.finish_commit((repo_name, commit1.id))

    commit2 = pfs_client.start_commit(repo_name, parent=commit1.id)
    assert commit2.repo.name == repo_name

def test_start_commit_on_branch_with_parent():
    pfs_client, repo_name = sandbox("start_commit_on_branch_with_parent")

    commit1 = pfs_client.start_commit(repo_name, branch='master')
    pfs_client.finish_commit((repo_name, commit1.id))

    commit2 = pfs_client.start_commit(repo_name, branch='master', parent=commit1.id)
    assert commit2.repo.name == repo_name


def test_start_commit_fork():
    pfs_client, repo_name = sandbox("start_commit_fork")

    commit1 = pfs_client.start_commit(repo_name, branch='master')
    pfs_client.finish_commit((repo_name, commit1.id))

    commit2 = pfs_client.start_commit(repo_name, branch='patch', parent='master')

    assert commit2.repo.name == repo_name

    branches = [branch_info.name for branch_info in pfs_client.list_branch(repo_name)]
    assert 'master' in branches
    assert 'patch' in branches


@pytest.mark.parametrize('commit_arg', ['commit_obj', 'repo/commit_id', '(repo, commit_id)'])
def test_finish_commit(commit_arg):
    pfs_client, repo_name = sandbox("finish_commit")
    commit = pfs_client.start_commit(repo_name)

    if commit_arg == 'commit_obj':
        pfs_client.finish_commit(commit)
    elif commit_arg == 'repo/commit_id':
        pfs_client.finish_commit('{}/{}'.format(repo_name, commit.id))
    elif commit_arg == '(repo, commit_id)':
        pfs_client.finish_commit((repo_name, commit.id))

    commit_infos = list(pfs_client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == commit.id

    commit_match_count = len([c for c in commit_infos if c.commit.id == commit.id and c.finished.seconds != 0])
    assert commit_infos[0].finished.seconds != 0
    assert commit_infos[0].finished.nanos != 0


def test_commit_context_mgr():
    """ Start and finish a commit using a context manager. """

    pfs_client, repo_name = sandbox("commit_context_mgr")

    with pfs_client.commit(repo_name, "master") as c1:
        pass
    with pfs_client.commit(repo_name, None) as c2:
        pass

    with pytest.raises(Exception):
        with pfs_client.commit("some-fake-repo", "master"):
            pass

    commit_infos = list(pfs_client.list_commit(repo_name))
    assert len(commit_infos) == 2
    assert sorted([c.commit.id for c in commit_infos]) == sorted([c1.id, c2.id])

def test_put_file_bytes_bytestring():
    """
    Start and finish a commit using a context manager while putting a file
    from a bytesting.
    """

    pfs_client, repo_name = sandbox("put_file_bytes_bytestring")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', b'DATA')

    commit_infos = list(pfs_client.list_commit(repo_name))
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    files = list(pfs_client.list_file('{}/{}'.format(repo_name, c.id), '.'))
    assert len(files) == 1


def test_put_file_bytes_bytestring_with_overwrite():
    """
    Start and finish a commit using a context manager while putting a file
    from a bytesting.
    """

    pfs_client, repo_name = sandbox("put_file_bytes_bytestring_with_overwrite")

    with pfs_client.commit(repo_name, 'mybranch') as c:
        for i in range(5):
            pfs_client.put_file_bytes(c, 'file.dat', b'DATA')

    with pfs_client.commit(repo_name, 'mybranch') as c:
        pfs_client.put_file_bytes(c, 'file.dat', b'FOO', overwrite_index=2)

    file = list(pfs_client.get_file('{}/{}'.format(repo_name, c.id), 'file.dat'))
    assert file == [b'DATA', b'DATA', b'FOO']


def test_put_file_bytes_filelike():
    """
    Start and finish a commit using a context manager while putting a file
    from a file-like object.
    """

    pfs_client, repo_name = sandbox("put_file_bytes_filelike")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', BytesIO(b'DATA'))


    files = list(pfs_client.list_file('{}/{}'.format(repo_name, c.id), '.'))
    assert len(files) == 1


def test_put_file_bytes_iterable():
    """
    Start and finish a commit using a context manager while putting a file
    from an iterator of bytes.
    """

    pfs_client, repo_name = sandbox("put_file_bytes_iterable")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', [b'DATA'])

    files = list(pfs_client.list_file('{}/{}'.format(repo_name, c.id), '.'))
    assert len(files) == 1


def test_put_file_url():
    pfs_client, repo_name = sandbox("put_file_url")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_url(c, "index.html", "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html")

    files = list(pfs_client.list_file('{}/{}'.format(repo_name, c.id), '.'))
    assert len(files) == 1
    assert files[0].file.path == '/index.html'


def test_copy_file():
    pfs_client, repo_name = sandbox("copy_file")

    with pfs_client.commit(repo_name, "master") as src_commit:
        pfs_client.put_file_bytes(src_commit, 'file1.dat', BytesIO(b'DATA1'))
        pfs_client.put_file_bytes(src_commit, 'file2.dat', BytesIO(b'DATA2'))

    with pfs_client.commit(repo_name, "master") as dest_commit:
        pfs_client.copy_file(src_commit, 'file1.dat', dest_commit, 'copy.dat')
        pfs_client.copy_file(src_commit, 'file2.dat', dest_commit, 'copy.dat', overwrite=True)

    files = list(pfs_client.list_file('{}/{}'.format(repo_name, dest_commit.id), '.'))
    assert len(files) == 3
    assert files[0].file.path == '/copy.dat'
    assert files[1].file.path == '/file1.dat'
    assert files[2].file.path == '/file2.dat'


def test_flush_commit():
    """
    Ensure flush commit works
    """

    pfs_client, repo_name = sandbox("flush_commit")

    with pfs_client.commit(repo_name, 'master') as c:
        pfs_client.put_file_bytes(c, 'input.json', b'hello world')

    # Just block until all of the commits are yielded
    list(pfs_client.flush_commit(['{}/{}'.format(repo_name, c.id)]))

    files = list(pfs_client.list_file('{}/master'.format(repo_name), '/'))
    assert len(files) == 1

def test_inspect_commit():
    pfs_client, repo_name = sandbox("inspect_commit")

    with pfs_client.commit(repo_name, 'master') as c:
        pfs_client.put_file_bytes(c, 'input.json', b'hello world')

    commit = pfs_client.inspect_commit("{}/master".format(repo_name))
    assert commit.branch.name == "master"
    assert commit.finished
    assert commit.description == ""
    assert commit.size_bytes == 11
    assert len(commit.commit.id) == 32
    assert commit.commit.repo.name == repo_name

def test_delete_commit():
    pfs_client, repo_name = sandbox("delete_commit")

    with pfs_client.commit(repo_name, 'master') as c:
        pass

    commits = list(pfs_client.list_commit(repo_name))
    assert len(commits) == 1
    pfs_client.delete_commit("{}/master".format(repo_name))
    commits = list(pfs_client.list_commit(repo_name))
    assert len(commits) == 0

def test_subscribe_commit():
    pfs_client, repo_name = sandbox("subscribe_commit")
    commits = pfs_client.subscribe_commit(repo_name, "master")

    with pfs_client.commit(repo_name, 'master') as c:
        pass

    commit = next(commits)
    assert commit.branch.repo.name == repo_name
    assert commit.branch.name == "master"

def test_list_branch():
    pfs_client, repo_name = sandbox("list_branch")

    with pfs_client.commit(repo_name, 'master') as c:
        pass
    with pfs_client.commit(repo_name, 'develop') as c:
        pass

    branches = pfs_client.list_branch(repo_name)
    assert len(branches) == 2
    assert branches[0].name == "develop"
    assert branches[1].name == "master"

def test_delete_branch():
    pfs_client, repo_name = sandbox("delete_branch")

    with pfs_client.commit(repo_name, 'develop') as c:
        pass

    branches = pfs_client.list_branch(repo_name)
    assert len(branches) == 1
    pfs_client.delete_branch(repo_name, "develop")
    branches = pfs_client.list_branch(repo_name)
    assert len(branches) == 0

def test_inspect_file():
    pfs_client, repo_name = sandbox("inspect_file")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', [b'DATA'])

    fi = pfs_client.inspect_file(c, 'file.dat')
    assert fi.file.commit.id == c.id
    assert fi.file.commit.repo.name == repo_name
    assert fi.file.path == 'file.dat'
    assert fi.size_bytes == 4
    assert fi.objects[0].hash == '4ba7d4149c32f5ccc6e54190beef0f503d1e637249baa9e4b123f5aa5c89506f299c10a7e32ab1e4bae30ed32df848f87d9b03a640320b0ca758c5ee56cb2db4'

def test_list_file():
    pfs_client, repo_name = sandbox("list_file")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file1.dat', [b'DATA'])
        pfs_client.put_file_bytes(c, 'file2.dat', [b'DATA'])

    files = list(pfs_client.list_file(c, '/'))
    assert len(files) == 2
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FILE
    assert files[0].file.path == "/file1.dat"
    assert files[1].size_bytes == 4
    assert files[1].file_type == python_pachyderm.FILE
    assert files[1].file.path == "/file2.dat"

def test_walk_file():
    pfs_client, repo_name = sandbox("walk_file")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, '/file1.dat', [b'DATA'])
        pfs_client.put_file_bytes(c, '/a/file2.dat', [b'DATA'])
        pfs_client.put_file_bytes(c, '/a/b/file3.dat', [b'DATA'])

    files = list(pfs_client.walk_file(c, '/a'))
    assert len(files) == 4
    assert files[0].file.path == '/a'
    assert files[1].file.path == '/a/b'
    assert files[2].file.path == '/a/b/file3.dat'
    assert files[3].file.path == '/a/file2.dat'

def test_glob_file():
    pfs_client, repo_name = sandbox("glob_file")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file1.dat', [b'DATA'])
        pfs_client.put_file_bytes(c, 'file2.dat', [b'DATA'])

    files = list(pfs_client.glob_file(c, '/*.dat'))
    assert len(files) == 2
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FILE
    assert files[0].file.path == "/file1.dat"
    assert files[1].size_bytes == 4
    assert files[1].file_type == python_pachyderm.FILE
    assert files[1].file.path == "/file2.dat"

    files = list(pfs_client.glob_file(c, '/*1.dat'))
    assert len(files) == 1
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FILE
    assert files[0].file.path == "/file1.dat"

def test_delete_file():
    pfs_client, repo_name = sandbox("delete_file")

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file1.dat', [b'DATA'])

    assert len(list(pfs_client.list_file(c, '/'))) == 1

    with pfs_client.commit(repo_name) as c:
        pfs_client.delete_file(c, 'file1.dat')

    assert len(list(pfs_client.list_file(c, '/'))) == 0

def test_create_branch():
    pfs_client, repo_name = sandbox("create_branch")
    pfs_client.create_branch(repo_name, "foobar")
    branches = pfs_client.list_branch(repo_name)
    assert len(branches) == 1
    assert branches[0].name == "foobar"

def test_inspect_branch():
    pfs_client, repo_name = sandbox("inspect_branch")
    pfs_client.create_branch(repo_name, "foobar")
    branch = pfs_client.inspect_branch(repo_name, "foobar")
    print(branch)
