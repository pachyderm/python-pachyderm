#!/usr/bin/env python

"""Tests admin-related functionality"""

import os
import pytest

import python_pachyderm
from python_pachyderm.service import enterprise_proto, identity_proto
from tests import util


@pytest.fixture
def client():
    pc = python_pachyderm.Client()
    pc.delete_all_license()

    pc.activate_license(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    pc.add_cluster("localhost", "localhost:1650", secret="secret")
    pc.update_cluster("localhost", "localhost:1650", "localhost:16650")
    pc.activate_enterprise("localhost:1650", "localhost", "secret")

    pc.auth_token = "iamroot"
    pc.activate_auth(pc.auth_token)
    pc.set_identity_server_config(
        config=identity_proto.IdentityServerConfig(issuer="http://localhost:1658")
    )

    yield pc

    pc.delete_cluster("localhost")
    pc.deactivate_enterprise()
    pc.delete_all()


@util.skip_if_no_enterprise()
def test_enterprise(client):
    assert len(client.list_clusters()) == len(client.list_user_clusters())
    assert client.get_enterprise_state().state == enterprise_proto.State.ACTIVE
    assert (
        client.get_activation_code().activation_code
        == os.environ["PACH_PYTHON_ENTERPRISE_CODE"]
    )
    assert (
        client.get_pause_status().status
        == enterprise_proto.PauseStatusResponse.PauseStatus.UNPAUSED
    )
