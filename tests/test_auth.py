#!/usr/bin/env python

"""Tests admin-related functionality"""

import os
from contextlib import contextmanager
from time import sleep

import pytest

import python_pachyderm
from tests import util
from python_pachyderm.service import auth_proto, identity_proto


@pytest.fixture
def client():
    pc = python_pachyderm.Client()
    pc.activate_license(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    pc.add_cluster("localhost", "localhost:1650", secret="secret")
    pc.activate_enterprise("localhost:1650", "localhost", "secret")

    pc.auth_token = "iamroot"
    pc.activate_auth(pc.auth_token)
    pc.set_identity_server_config(
        config=identity_proto.IdentityServerConfig(issuer="http://localhost:1658")
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
        auth_proto.OIDCConfig(
            issuer="http://localhost:1658",
            client_id="client",
            client_secret="secret",
            redirect_uri="http://test.example.com",
        )
    )


@util.skip_if_no_enterprise()
def test_cluster_role_bindings(client):
    cluster_resource = auth_proto.Resource(type=auth_proto.CLUSTER)
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
        auth_proto.Resource(type=auth_proto.REPO, name="foobar"),
        [auth_proto.Permission.REPO_READ],
    )


@util.skip_if_no_enterprise()
def test_who_am_i(client):
    assert client.who_am_i().username == "pach:root"


@util.skip_if_no_enterprise()
def test_get_roles_for_permission(client):
    # Checks built-in roles
    roles = client.get_roles_for_permission(auth_proto.Permission.REPO_READ)
    for r in roles.roles:
        assert auth_proto.Permission.REPO_READ in r.permissions

    roles = client.get_roles_for_permission(
        auth_proto.Permission.CLUSTER_GET_PACHD_LOGS
    )
    for r in roles.roles:
        assert auth_proto.Permission.CLUSTER_GET_PACHD_LOGS in r.permissions


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
