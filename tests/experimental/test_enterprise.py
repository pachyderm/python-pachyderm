#!/usr/bin/env python

"""Tests admin-related functionality"""
import os

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.enterprise import State
from tests import util


@util.skip_if_no_enterprise()
def test_enterprise():
    client = ExperimentalClient()
    client.license.delete_all()
    client.license.activate(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    client.license.add_cluster("localhost", "localhost:1650", secret="secret")
    client.enterprise.activate("localhost:1650", "localhost", "secret")
    assert client.enterprise.get_state().state == State.ACTIVE
    client.enterprise.deactivate()
    client.license.delete_all()
