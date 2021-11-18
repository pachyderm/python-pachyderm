#!/usr/bin/env python

"""Tests admin-related functionality"""

import python_pachyderm
from python_pachyderm.experimental.service import admin_proto


def test_inspect_cluster():
    client = python_pachyderm.experimental.Client()
    res = client.inspect_cluster()
    assert isinstance(res, admin_proto.ClusterInfo)
