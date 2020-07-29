#!/usr/bin/env python

"""Tests admin-related functionality"""

import os

import pytest

import python_pachyderm
from tests import util


@util.skip_if_no_enterprise()
def test_enterprise():
    client = python_pachyderm.Client()
    client.activate_enterprise(os.environ["PACH_PYTHON_ENTERPRISE_CODE"])
    assert client.get_enterprise_state().state == python_pachyderm.State.ACTIVE.value
    client.deactivate_enterprise()
