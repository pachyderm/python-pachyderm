#!/usr/bin/env python

"""Tests admin-related functionality"""

import os

import python_pachyderm
from python_pachyderm.service import enterprise_proto
from tests import util


@util.skip_if_no_enterprise()
def test_enterprise():
    client = python_pachyderm.Client()
    client.delete_all_license()

    client.activate_license(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    client.add_cluster("localhost", "localhost:1650", secret="secret")
    client.update_cluster("localhost", "localhost:1650", "localhost:16650")
    client.activate_enterprise("localhost:1650", "localhost", "secret")

    assert len(client.list_clusters()) == len(client.list_user_clusters())
    assert client.get_enterprise_state().state == enterprise_proto.State.ACTIVE
    assert (
        client.get_activation_code().activation_code
        == os.environ["PACH_PYTHON_ENTERPRISE_CODE"]
    )

    client.delete_cluster("localhost")
    client.deactivate_enterprise()
    client.delete_all_license()
