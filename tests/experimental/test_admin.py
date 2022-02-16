#!/usr/bin/env python

"""Tests admin-related functionality"""
from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.admin import ClusterInfo


def test_inspect_cluster():
    client = ExperimentalClient()
    res = client.admin.inspect_cluster()

    assert isinstance(res, ClusterInfo)
