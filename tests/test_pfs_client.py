#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `PfsClient` class of the `python_pachyderm` package."""


from collections import namedtuple
try:
    from StringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

import six
import pytest
import threading

import python_pachyderm


@pytest.fixture(scope='function')
def pfs_client():
    """Connect to Pachyderm before tests and reset to initial state after tests."""
    # Setup : create a PfsClient instance
    client = python_pachyderm.PfsClient()

    yield client  # this is where the testing happens

    # Teardown : Delete all repos, commits, files, pipelines and jobs.  This resets the cluster to its initial state.
    client.delete_all()


@pytest.fixture(scope='function')
def pfs_client_with_repo():
    """Connect to Pachyderm before tests and reset to initial state after tests."""
    # Setup : create a PfsClient instance and create a repository
    client = python_pachyderm.PfsClient()
    client.create_repo('test-repo-1', 'This is a test repository')

    yield client, 'test-repo-1'  # this is where the testing happens

    # Teardown : Delete all repos, commits, files, pipelines and jobs.  This resets the cluster to its initial state.
    client.delete_all()


def test_pfs_client_init_with_default_host_port():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created without specifying a host or port
    client = python_pachyderm.PfsClient()
    # THEN the GRPC channel should reflect the default of localhost and port 30650
    assert client.channel._channel.target() == b'localhost:30650'


def test_pfs_client_init_with_env_vars(monkeypatch):
    # GIVEN a Pachyderm deployment
    # WHEN environment variables are set for Pachyderm host and port
    monkeypatch.setenv('PACHD_ADDRESS', 'pachd.example.com:12345')
    #   AND a client is created without specifying a host or port
    client = python_pachyderm.PfsClient()
    # THEN the GRPC channel should reflect the host and port specified in the environment variables
    assert client.channel._channel.target() == b'pachd.example.com:12345'


def test_pfs_client_init_with_args():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created with host and port arguments
    client = python_pachyderm.PfsClient(host='pachd.example.com', port=54321)
    # THEN the GRPC channel should reflect the host and port specified in the arguments
    assert client.channel._channel.target() == b'pachd.example.com:54321'


def test_pfs_inspect_repo(pfs_client_with_repo):
    client, repo_name = pfs_client_with_repo
    client.inspect_repo(repo_name)
    repo_info = client.list_repo()
    assert len(repo_info) == 1
    assert repo_info[0].repo.name == repo_name
    assert repo_info[0].description == "This is a test repository"
    assert repo_info[0].size_bytes == 0


def test_pfs_delete_repo(pfs_client_with_repo):
    # GIVEN a Pachyderm deployment
    #   AND a connected PFS client
    client, repo_name = pfs_client_with_repo
    #   AND one existing repo
    assert len(client.list_repo()) == 1
    # WHEN calling delete_repo() with the repo_name of the existing repo
    client.delete_repo(repo_name)
    # THEN no repositories should remain
    assert len(client.list_repo()) == 0


def test_pfs_delete_repo_raises(pfs_client):
    assert len(pfs_client.list_repo()) == 0

    with pytest.raises(ValueError) as excinfo:
        pfs_client.delete_repo()
    exception_msg = excinfo.value.args[0]

    assert exception_msg == 'Either a repo_name or all=True needs to be provided'


def test_pfs_delete_non_existent_repo_raises(pfs_client):
    assert len(pfs_client.list_repo()) == 0
    pfs_client.delete_repo('BOGUS_NAME')


def test_pfs_delete_all_repos(pfs_client):
    pfs_client.create_repo('test-repo-1')
    pfs_client.create_repo('test-repo-2')
    assert len(pfs_client.list_repo()) == 2

    pfs_client.delete_repo(all=True)
    assert len(pfs_client.list_repo()) == 0


def test_pfs_delete_all_repos_with_name_raises(pfs_client):
    pfs_client.create_repo('test-repo-1')
    pfs_client.create_repo('test-repo-2')
    assert len(pfs_client.list_repo()) == 2

    with pytest.raises(ValueError) as excinfo:
        pfs_client.delete_repo('test-repo-1', all=True)

    exception_msg = excinfo.value.args[0]
    assert exception_msg == 'Cannot specify a repo_name if all=True'
    assert len(pfs_client.list_repo()) == 2


@pytest.mark.parametrize('repo_to_create,repo_to_commit_to,branch', [
    ('test-repo-1', 'test-repo-1', 'master'),
    ('test-repo-1', 'test-repo-1', None),
    pytest.param('test-repo-1', 'non-existent-repo', 'master',
                 marks=[pytest.mark.basic, pytest.mark.xfail(strict=True)])
])
def test_pfs_start_commit(pfs_client, repo_to_create, repo_to_commit_to, branch):
    """ Start a commit in repo `test-repo-1` on branch `master`. """
    pfs_client.create_repo(repo_to_create)
    commit = pfs_client.start_commit(repo_to_commit_to, branch)
    assert commit.repo.name == repo_to_commit_to
    assert isinstance(commit.id, six.string_types)


def test_pfs_start_commit_missing_branch(pfs_client_with_repo):
    """ Start a new commit in repo `test-repo-1` that's not on any branch. """
    pfs_client, repo_name = pfs_client_with_repo
    commit = pfs_client.start_commit(repo_name)
    assert commit.repo.name == repo_name
    assert isinstance(commit.id, six.string_types)


def test_pfs_start_commit_missing_repo_name_raises(pfs_client):
    """ Trying to start a commit without specifying a repo name should raise an error. """
    with pytest.raises(TypeError) as excinfo:
        pfs_client.start_commit()


def test_pfs_start_commit_with_parent_no_branch(pfs_client_with_repo):
    """ Start a commit with an existing commit ID as the parent in repo `test-repo-1`, not on any branch. """
    # GIVEN a Pachyderm deployment in its initial state
    #   AND a connected PFS client
    pfs_client, repo_name = pfs_client_with_repo

    commit1 = pfs_client.start_commit(repo_name)
    pfs_client.finish_commit((repo_name, commit1.id))

    commit2 = pfs_client.start_commit(repo_name, parent=commit1.id)
    assert commit2.repo.name == repo_name
    assert isinstance(commit2.id, six.string_types)


def test_pfs_start_commit_on_branch_with_parent(pfs_client_with_repo):
    """ Start a commit with a previous commit as the parent in repo `test-repo-1`, on the `master` branch. """
    pfs_client, repo_name = pfs_client_with_repo
    branch = 'master'

    commit1 = pfs_client.start_commit(repo_name, branch=branch)
    pfs_client.finish_commit((repo_name, commit1.id))

    commit2 = pfs_client.start_commit(repo_name, branch=branch, parent=commit1.id)
    assert commit2.repo.name == repo_name
    assert isinstance(commit2.id, six.string_types)


def test_pfs_start_commit_fork(pfs_client_with_repo):
    """
    Start a commit with `master` as the parent in repo `test-repo-1`, on a new branch `patch`;
    essentially a fork.
    """
    pfs_client, repo_name = pfs_client_with_repo

    branch1 = 'master'
    commit1 = pfs_client.start_commit(repo_name, branch=branch1)
    pfs_client.finish_commit((repo_name, commit1.id))

    branch2 = 'patch'
    commit2 = pfs_client.start_commit(repo_name, branch=branch2, parent=branch1)

    assert commit2.repo.name == repo_name
    assert isinstance(commit2.id, six.string_types)

    branches = [branch_info.name for branch_info in pfs_client.list_branch(repo_name)]
    assert (branch1 in branches) and (branch2 in branches)


@pytest.mark.parametrize('commit_arg', ['commit_obj', 'repo/commit_id', '(repo, commit_id)'])
def test_pfs_finish_commit(pfs_client_with_repo, commit_arg):
    """ Finish a new commit in repo `test-repo-1` that's not on any branch. """
    pfs_client, repo_name = pfs_client_with_repo
    commit = pfs_client.start_commit(repo_name)

    if commit_arg == 'commit_obj':
        pfs_client.finish_commit(commit)
    elif commit_arg == 'repo/commit_id':
        pfs_client.finish_commit('{}/{}'.format(repo_name, commit.id))
    elif commit_arg == '(repo, commit_id)':
        pfs_client.finish_commit((repo_name, commit.id))

    commit_infos = pfs_client.list_commit(repo_name)
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == commit.id

    commit_match_count = len([c for c in commit_infos if c.commit.id == commit.id and c.finished.seconds != 0])
    assert commit_infos[0].finished.seconds != 0
    assert commit_infos[0].finished.nanos != 0


@pytest.mark.parametrize('repo_to_create,repo_to_commit_to,branch', [
    ('test-repo-1', 'test-repo-1', 'master'),
    ('test-repo-1', 'test-repo-1', None),
    pytest.param('test-repo-1', 'non-existent-repo', 'master',
                 marks=[pytest.mark.basic, pytest.mark.xfail(strict=True)])
])
def test_pfs_commit_context_mgr(pfs_client, repo_to_create, repo_to_commit_to, branch):
    """ Start and finish a commit using a context manager. """
    pfs_client.create_repo(repo_to_create)
    # WHEN calling the commit() context manager with the repo_name and branch specified
    with pfs_client.commit(repo_to_commit_to, branch) as c:
        pass
    # THEN a single commit should exist in the repo
    commit_infos = pfs_client.list_commit(repo_to_commit_to)
    assert len(commit_infos) == 1
    #   AND the commit ID should match the finished commit
    assert commit_infos[0].commit.id == c.id


def test_pfs_commit_context_mgr_missing_branch(pfs_client_with_repo):
    """ Start and finish a commit using a context manager. """

    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pass

    commit_infos = pfs_client.list_commit(repo_name)
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id


def test_put_file_bytes_bytestring(pfs_client_with_repo):
    """
    Start and finish a commit using a context manager while putting a file
    from a bytesting.
    """

    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', b'DATA')

    commit_infos = pfs_client.list_commit(repo_name)
    assert len(commit_infos) == 1
    assert commit_infos[0].commit.id == c.id
    files = pfs_client.get_files('{}/{}'.format(repo_name, c.id), '.')
    assert len(files) == 1


def test_put_file_bytes_bytestring_with_overwrite(pfs_client_with_repo):
    """
    Start and finish a commit using a context manager while putting a file
    from a bytesting.
    """

    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name, 'mybranch') as c:
        for i in range(5):
            pfs_client.put_file_bytes(c, 'file.dat', b'DATA')

    with pfs_client.commit(repo_name, 'mybranch') as c:
        pfs_client.put_file_bytes(c, 'file.dat', b'FOO', overwrite_index=2)

    file = list(pfs_client.get_file('{}/{}'.format(repo_name, c.id), 'file.dat'))
    assert file == [b'DATA', b'DATA', b'FOO']


def test_put_file_bytes_filelike(pfs_client_with_repo):
    """
    Start and finish a commit using a context manager while putting a file
    from a file-like object.
    """

    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', BytesIO(b'DATA'))

    files = pfs_client.get_files('{}/{}'.format(repo_name, c.id), '.')
    assert len(files) == 1


def test_put_file_bytes_iterable(pfs_client_with_repo):
    """
    Start and finish a commit using a context manager while putting a file
    from an iterator of bytes.
    """

    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', [b'DATA'])

    files = pfs_client.get_files('{}/{}'.format(repo_name, c.id), '.')
    assert len(files) == 1


def test_put_file_url(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_url(c, "index.html", "https://gist.githubusercontent.com/ysimonson/1986773831f6c4c292a7290c5a5d4405/raw/fb2b4d03d317816e36697a6864a9c27645baa6c0/wheel.html")

    files = pfs_client.get_files('{}/{}'.format(repo_name, c.id), '.')
    assert len(files) == 1
    assert '/index.html' in files


def test_flush_commit(pfs_client_with_repo):
    """
    Ensure flush commit works
    """

    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name, 'master') as c:
        pfs_client.put_file_bytes(c, 'input.json', b'hello world')

    # Just block until all of the commits are yielded
    list(pfs_client.flush_commit(['{}/{}'.format(repo_name, c.id)]))

    files = pfs_client.get_files('{}/master'.format(repo_name), '/', recursive=True)
    assert files == {'/input.json': b'hello world'}


def test_inspect_commit(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name, 'master') as c:
        pfs_client.put_file_bytes(c, 'input.json', b'hello world')

    commit = pfs_client.inspect_commit("{}/master".format(repo_name))
    assert commit.branch.name == "master"
    assert commit.finished
    assert commit.description == ""
    assert commit.size_bytes == 11
    assert len(commit.commit.id) == 32
    assert commit.commit.repo.name == "test-repo-1"

def test_delete_commit(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name, 'master') as c:
        pass

    assert len(pfs_client.list_commit(repo_name)) == 1
    pfs_client.delete_commit("{}/master".format(repo_name))
    assert len(pfs_client.list_commit(repo_name)) == 0

def test_subscribe_commit(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo
    commits = pfs_client.subscribe_commit(repo_name, "master")

    with pfs_client.commit(repo_name, 'master') as c:
        pass

    commit = next(commits)
    assert commit.branch.repo.name == repo_name
    assert commit.branch.name == "master"

def test_list_branch(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name, 'master') as c:
        pass
    with pfs_client.commit(repo_name, 'develop') as c:
        pass

    branches = pfs_client.list_branch(repo_name)
    assert len(branches) == 2
    assert branches[0].name == "develop"
    assert branches[1].name == "master"

def test_delete_branch(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name, 'develop') as c:
        pass

    branches = pfs_client.list_branch(repo_name)
    assert len(branches) == 1
    pfs_client.delete_branch(repo_name, "develop")
    branches = pfs_client.list_branch(repo_name)
    assert len(branches) == 0

def test_inspect_file(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file.dat', [b'DATA'])

    fi = pfs_client.inspect_file(c, 'file.dat')
    assert fi.file.commit.id == c.id
    assert fi.file.commit.repo.name == repo_name
    assert fi.file.path == 'file.dat'
    assert fi.size_bytes == 4
    assert fi.objects[0].hash == '4ba7d4149c32f5ccc6e54190beef0f503d1e637249baa9e4b123f5aa5c89506f299c10a7e32ab1e4bae30ed32df848f87d9b03a640320b0ca758c5ee56cb2db4'

def test_list_file(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file1.dat', [b'DATA'])
        pfs_client.put_file_bytes(c, 'file2.dat', [b'DATA'])

    files = pfs_client.list_file(c, '/')
    assert len(files) == 2
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FILE
    assert files[0].file.path == "/file1.dat"
    assert files[1].size_bytes == 4
    assert files[1].file_type == python_pachyderm.FILE
    assert files[1].file.path == "/file2.dat"

def test_list_file_recursive(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo
    expected_files = set()

    with pfs_client.commit(repo_name) as c:
        for i in range(10):
            filename = '{}/{}'.format(i % 2, i)
            pfs_client.put_file_bytes(c, filename, [b'DATA'])
            expected_files.add('/{}'.format(filename))

    files = pfs_client.list_file(c, '/', recursive=True)
    assert len(files) == 10

    for f in files:
        assert f.size_bytes == 4
        assert f.file_type == python_pachyderm.FILE
        assert f.file.path in expected_files

def test_glob_file(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file1.dat', [b'DATA'])
        pfs_client.put_file_bytes(c, 'file2.dat', [b'DATA'])

    files = pfs_client.glob_file(c, '/*.dat')
    assert len(files) == 2
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FILE
    assert files[0].file.path == "/file1.dat"
    assert files[1].size_bytes == 4
    assert files[1].file_type == python_pachyderm.FILE
    assert files[1].file.path == "/file2.dat"

    files = pfs_client.glob_file(c, '/*1.dat')
    assert len(files) == 1
    assert files[0].size_bytes == 4
    assert files[0].file_type == python_pachyderm.FILE
    assert files[0].file.path == "/file1.dat"

def test_delete_file(pfs_client_with_repo):
    pfs_client, repo_name = pfs_client_with_repo

    with pfs_client.commit(repo_name) as c:
        pfs_client.put_file_bytes(c, 'file1.dat', [b'DATA'])

    assert len(pfs_client.list_file(c, '/')) == 1

    with pfs_client.commit(repo_name) as c:
        pfs_client.delete_file(c, 'file1.dat')

    assert len(pfs_client.list_file(c, '/')) == 0
