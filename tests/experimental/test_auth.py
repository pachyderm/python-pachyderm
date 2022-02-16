#!/usr/bin/env python

"""Tests admin-related functionality"""
import os

import grpc
import pytest

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.auth import (
    OidcConfig,
    Permission,
    Resource,
    ResourceType,
)
from python_pachyderm.experimental.mixin.identity import IdentityServerConfig
from tests import util

# bp_to_pb: OidcConfig -> OIDCConfig


@pytest.fixture
def client():
    pc = ExperimentalClient()
    pc.license.activate(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    pc.license.add_cluster("localhost", "localhost:1650", secret="secret")
    pc.enterprise.activate("localhost:1650", "localhost", "secret")

    pc.auth_token = "iamroot"
    pc.auth.activate(pc.auth_token)
    pc.identity.set_identity_server_config(
        config=IdentityServerConfig(issuer="http://localhost:1658")
    )
    yield pc
    # not redundant because auth_token could be overriden by tests
    pc.auth_token = "iamroot"
    try:
        pc.identity.delete_all()
    except Exception:
        pass
    try:
        pc.license.delete_all()
    except Exception:
        pass
    try:
        pc.auth.deactivate()
    except Exception:
        pass
    pc.enterprise.deactivate()


@util.skip_if_no_enterprise()
def test_auth_configuration(client):
    client.auth.get_configuration()
    client.auth.set_configuration(
        OidcConfig(
            issuer="http://localhost:1658",
            client_id="client",
            client_secret="secret",
            redirect_uri="http://test.example.com",
        )
    )


@util.skip_if_no_enterprise()
def test_cluster_role_bindings(client):
    cluster_resource = Resource(type=ResourceType.CLUSTER)
    binding = client.auth.get_role_binding(cluster_resource)
    assert binding["pach:root"].roles["clusterAdmin"]
    client.auth.modify_role_binding(
        cluster_resource, "robot:someuser", roles=["clusterAdmin"]
    )

    binding = client.auth.get_role_binding(cluster_resource)
    assert binding["robot:someuser"].roles["clusterAdmin"]


@util.skip_if_no_enterprise()
def test_authorize(client):
    client.auth.authorize(
        Resource(type=ResourceType.REPO, name="foobar"),
        [Permission.REPO_READ],
    )


@util.skip_if_no_enterprise()
def test_who_am_i(client):
    assert client.auth.who_am_i().username == "pach:root"


@util.skip_if_no_enterprise()
def test_get_roles_for_permission(client):
    # Checks built-in roles
    roles = client.auth.get_roles_for_permission(Permission.REPO_READ)
    for r in roles:
        assert Permission.REPO_READ in r.permissions

    roles = client.auth.get_roles_for_permission(Permission.CLUSTER_GET_PACHD_LOGS)
    for r in roles:
        assert Permission.CLUSTER_GET_PACHD_LOGS in r.permissions


@util.skip_if_no_enterprise()
def test_robot_token(client):
    auth_token = client.auth.get_robot_token("robot:root", ttl=30)
    client.auth_token = auth_token
    assert client.auth.who_am_i().username == "robot:root"
    client.auth.revoke_auth_token(auth_token)
    with pytest.raises(grpc.RpcError):
        client.auth.who_am_i()


@util.skip_if_no_enterprise()
def test_groups(client):
    assert client.auth.get_groups() == []
    client.auth.set_groups_for_user("pach:root", ["foogroup"])
    assert client.auth.get_groups() == ["foogroup"]
    assert client.auth.get_users("foogroup") == ["pach:root"]
    client.auth.modify_members("foogroup", remove=["pach:root"])
    assert client.auth.get_groups() == []
    assert client.auth.get_users("foogroup") == []
