#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `PfsClient` class of the `pypachy` package."""


from collections import namedtuple
import os
import pytest
import pypachy


@pytest.fixture(scope='function')
def pfs_client():
    """Connect to Pachyderm before tests and reset to initial state after tests."""
    # Setup : create a PfsClient instance
    client = pypachy.PfsClient()

    yield client  # this is where the testing happens

    # Teardown : Delete all repos, commits, files, pipelines and jobs.  This resets the cluster to its initial state.
    client.delete_all()


# Repository types : [name: str, description: str]
Repo = namedtuple('Repo', ['name', 'description'])

# Repository test examples
repos_to_test = (Repo(name='test', description='This is a test repository'),
                 Repo(name='test2', description=''))


@pytest.fixture(scope='function', params=repos_to_test)
def pfs_client_with_repos(request):
    """Connect to Pachyderm before tests and reset to initial state after tests."""
    # Setup : create a PfsClient instance and create a repository
    client = pypachy.PfsClient()
    client.create_repo(request.param.name, request.param.description)

    yield client, request.param  # this is where the testing happens

    # Teardown : Delete all repos, commits, files, pipelines and jobs.  This resets the cluster to its initial state.
    client.delete_all()


def test_pfs_client_init_with_default_host_port():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created without specifying a host or port
    client = pypachy.PfsClient()
    # THEN the GRPC channel should reflect the default of localhost and port 30650
    assert client.channel._channel.target() == b'localhost:30650'


# Temporarily disable test until this issue is solved: https://github.com/pytest-dev/pytest/issues/3071
# def test_pfs_client_init_with_env_vars(monkeypatch):
#     # GIVEN a Pachyderm deployment
#     # WHEN environment variables are set for Pachyderm host and port
#     monkeypatch.setenv('PACHD_SERVICE_HOST', 'pachd.example.com')
#     monkeypatch.setenv('PACHD_SERVICE_PORT_API_GRPC_PORT', '12345')
#     #   AND a client is created without specifying a host or port
#     client = pypachy.PfsClient()
#     # THEN the GRPC channel should reflect the host and port specified in the environment variables
#     assert client.channel._channel.target() == b'pachd.example.com:12345'


def test_pfs_client_init_with_args():
    # GIVEN a Pachyderm deployment
    # WHEN a client is created with host and port arguments
    client = pypachy.PfsClient(host='pachd.example.com', port=54321)
    # THEN the GRPC channel should reflect the host and port specified in the arguments
    assert client.channel._channel.target() == b'pachd.example.com:54321'


def test_pfs_list_repo(pfs_client):
    # GIVEN a Pachyderm deployment in its initial state
    #   AND a connected PFS client
    client = pfs_client
    # WHEN calling list_repo()
    repo_info = client.list_repo()
    # THEN an empty list should be returned
    assert list(repo_info) == []


def test_pfs_create_repo(pfs_client):
    # GIVEN a Pachyderm deployment in its initial state
    #   AND a connected PFS client
    client = pfs_client
    # WHEN calling create_repo() with a name but no description
    repo_name = 'test'
    client.create_repo(repo_name)
    #   AND calling list_repo()
    repo_info = client.list_repo()
    # THEN only one repository should exist
    assert len(repo_info) == 1
    #   AND the repo name should match the specified value
    assert repo_info[0].repo.name == repo_name
    #   AND the description should be empty
    assert repo_info[0].description == ''
    #   AND the size in Bytes should be 0
    assert repo_info[0].size_bytes == 0
    #   AND provenance should be empty
    assert len(repo_info[0].provenance) == 0


def test_pfs_create_repo_with_description(pfs_client):
    # GIVEN a Pachyderm deployment in its initial state
    #   AND a connected PFS client
    client = pfs_client
    # WHEN calling create_repo() with a name and description
    repo_name = 'test'
    repo_description = 'This is a test repository'
    client.create_repo(repo_name, repo_description)
    #   AND calling list_repo()
    repo_info = client.list_repo()
    # THEN only one repository should exist
    assert len(repo_info) == 1
    #   AND the repo name should match the specified value
    assert repo_info[0].repo.name == repo_name
    #   AND the description should match the specified value
    assert repo_info[0].description == repo_description
    #   AND the size in Bytes should be 0
    assert repo_info[0].size_bytes == 0
    #   AND provenance should be empty
    assert len(repo_info[0].provenance) == 0


def test_pfs_inspect_repo(pfs_client_with_repos):
    # GIVEN a Pachyderm deployment in its initial state
    #   AND a connected PFS client
    client, test_repo = pfs_client_with_repos
    #   AND an existing repository with a name but no description
    # WHEN calling inspect_repo() with a name but no description
    client.inspect_repo(test_repo.name)
    #   AND calling list_repo()
    repo_info = client.list_repo()
    # THEN only one repository should exist
    assert len(repo_info) == 1
    #   AND the repo name should match the specified value
    assert repo_info[0].repo.name == test_repo.name
    #   AND the description should be empty
    assert repo_info[0].description == test_repo.description
    #   AND the size in Bytes should be 0
    assert repo_info[0].size_bytes == 0
    #   AND provenance should be empty
    assert len(repo_info[0].provenance) == 0

