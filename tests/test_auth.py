#!/usr/bin/env python

"""Tests admin-related functionality"""

import os
from contextlib import contextmanager
from time import sleep

import pytest

import python_pachyderm
from tests import util
from python_pachyderm.proto.auth import auth_pb2
from python_pachyderm.proto.identity import identity_pb2

@contextmanager
def sandbox():
    client = python_pachyderm.Client()
    client.activate_license(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    client.add_cluster("localhost", "localhost:650", secret="secret")
    client.activate_enterprise("localhost:650", "localhost", "secret")
    root_auth_token = None

    try:
        client.auth_token = "iamroot"
        client.activate_auth(client.auth_token)
        client.set_identity_server_config(config=identity_pb2.IdentityServerConfig(issuer="http://localhost:658"))
        yield client
    finally:
        client.auth_token = "iamroot"
        try:
            client.delete_all_identity()
        except:
            pass
        try:
            client.delete_all_license()
        except:
            pass
        try:
            client.deactivate_auth()
        except:
            pass
        client.deactivate_enterprise()

@util.skip_if_no_enterprise()
def test_auth_configuration():
    with sandbox() as client:
        config = client.get_auth_configuration()
        client.set_auth_configuration(auth_pb2.OIDCConfig(issuer="http://localhost:658", client_id="client", client_secret="secret", redirect_uri="http://test.example.com"))

@util.skip_if_no_enterprise()
def test_cluster_role_bindings():
    with sandbox() as client:
        cluster_resource = auth_pb2.Resource(type=auth_pb2.CLUSTER)
        binding = client.get_role_binding(cluster_resource)
        assert binding.binding.entries["pach:root"].roles["clusterAdmin"]
        client.modify_role_binding(cluster_resource, "robot:someuser", roles=["clusterAdmin"])

        binding = client.get_role_binding(cluster_resource)
        assert binding.binding.entries["robot:someuser"].roles["clusterAdmin"]
        
@util.skip_if_no_enterprise()
def test_authorize():
    with sandbox() as client:
        assert client.authorize(auth_pb2.Resource(type=auth_pb2.REPO, name="foobar"), [python_pachyderm.Permission.REPO_READ.value])

@util.skip_if_no_enterprise()
def test_who_am_i():
    with sandbox() as client:
        i = client.who_am_i()
        assert i.username == "pach:root"

@util.skip_if_no_enterprise()
def test_robot_token():
    with sandbox() as client:
        auth_token = client.get_robot_token("robot:root", ttl=30)
        client.auth_token = auth_token
        assert client.who_am_i().username == "robot:root"
        client.revoke_auth_token(auth_token)
        with pytest.raises(python_pachyderm.RpcError):
            client.who_am_i()

@util.skip_if_no_enterprise()
def test_groups():
    with sandbox() as client:
        assert client.get_groups() == []
        client.set_groups_for_user("pach:root", ["foogroup"])
        assert client.get_groups() == ["foogroup"]
        assert client.get_users("foogroup") == ["pach:root"]
        client.modify_members("foogroup", remove=["pach:root"])
        assert client.get_groups() == []
        assert client.get_users("foogroup") == []
