#!/usr/bin/env python

"""Tests admin-related functionality"""

import python_pachyderm
from python_pachyderm.proto.v2.admin import admin_pb2


def test_inspect_cluster():
    client = python_pachyderm.Client()
    res = client.inspect_cluster()
    assert isinstance(res, admin_pb2.ClusterInfo)
