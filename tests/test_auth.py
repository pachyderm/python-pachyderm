#!/usr/bin/env python

"""Tests admin-related functionality"""

import os
from contextlib import contextmanager
from time import sleep

import pytest

import python_pachyderm
from tests import util
from python_pachyderm.proto.v2.auth import auth_pb2
from python_pachyderm.proto.v2.identity import identity_pb2


@pytest.fixture
def client():
    pc = python_pachyderm.Client()
    pc.activate_license(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    pc.add_cluster("localhost", "localhost:650", secret="secret")
    pc.activate_enterprise("localhost:650", "localhost", "secret")

    pc.auth_token = "iamroot"
    pc.activate_auth(pc.auth_token)
    pc.set_identity_server_config(
        config=identity_pb2.IdentityServerConfig(issuer="http://localhost:658")
    )
    yield pc
    # not redundant because auth_token could be overriden by tests
    pc.auth_token = "iamroot"
    try:
        pc.delete_all_identity()
    except Exception:
        pass
    try:
        pc.delete_all_license()
    except Exception:
        pass
    try:
        pc.deactivate_auth()
    except Exception:
        pass
    pc.deactivate_enterprise()


@util.skip_if_no_enterprise()
def test_auth_configuration(client):
    client.get_auth_configuration()
    client.set_auth_configuration(
        auth_pb2.OIDCConfig(
            issuer="http://localhost:658",
            client_id="client",
            client_secret="secret",
            redirect_uri="http://test.example.com",
        )
    )


@util.skip_if_no_enterprise()
def test_cluster_role_bindings(client):
    cluster_resource = auth_pb2.Resource(type=auth_pb2.CLUSTER)
    binding = client.get_role_binding(cluster_resource)
    assert binding.binding.entries["pach:root"].roles["clusterAdmin"]
    client.modify_role_binding(
        cluster_resource, "robot:someuser", roles=["clusterAdmin"]
    )

    binding = client.get_role_binding(cluster_resource)
    assert binding.binding.entries["robot:someuser"].roles["clusterAdmin"]


@util.skip_if_no_enterprise()
def test_authorize(client):
    client.authorize(
        auth_pb2.Resource(type=auth_pb2.REPO, name="foobar"),
        [python_pachyderm.Permission.REPO_READ.value],
    )


@util.skip_if_no_enterprise()
def test_who_am_i(client):
    assert client.who_am_i().username == "pach:root"


@util.skip_if_no_enterprise()
def test_robot_token(client):
    auth_token = client.get_robot_token("robot:root", ttl=30)
    client.auth_token = auth_token
    assert client.who_am_i().username == "robot:root"
    client.revoke_auth_token(auth_token)
    with pytest.raises(python_pachyderm.RpcError):
        client.who_am_i()


@util.skip_if_no_enterprise()
def test_groups(client):
    assert client.get_groups() == []
    client.set_groups_for_user("pach:root", ["foogroup"])
    assert client.get_groups() == ["foogroup"]
    assert client.get_users("foogroup") == ["pach:root"]
    client.modify_members("foogroup", remove=["pach:root"])
    assert client.get_groups() == []
    assert client.get_users("foogroup") == []
