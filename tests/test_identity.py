#!/usr/bin/env python

"""Tests identity-related functionality"""

import os

import pytest

import python_pachyderm
from tests import util
from python_pachyderm.service import identity_proto


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
def test_identity_server_config(client):
    isc = client.get_identity_server_config()
    assert isc.issuer == "http://localhost:1658"


@util.skip_if_no_enterprise()
def test_oidc_client(client):
    oidc1 = client.create_oidc_client(
        identity_proto.OIDCClient(id="oidc1", name="pach1", secret="secret1")
    )
    client.create_oidc_client(
        identity_proto.OIDCClient(id="oidc2", name="pach2", secret="secret2")
    )

    assert len(client.list_oidc_clients()) == 2

    client.update_oidc_client(
        identity_proto.OIDCClient(id="oidc1", name="pach3", secret="secret1")
    )
    oidc_updated = client.get_oidc_client(oidc1.id)
    assert oidc_updated.name == "pach3"

    client.delete_oidc_client(oidc1.id)
    assert len(client.list_oidc_clients()) == 1
