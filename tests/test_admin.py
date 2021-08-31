#!/usr/bin/env python

"""Tests admin-related functionality"""

import python_pachyderm
from python_pachyderm.service import admin_proto


def test_inspect_cluster():
    client = python_pachyderm.Client()
    res = client.inspect_cluster()
    assert isinstance(res, admin_proto.ClusterInfo)
