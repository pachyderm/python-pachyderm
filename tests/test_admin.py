#!/usr/bin/env python

"""Tests admin-related functionality"""

import pytest

import python_pachyderm
from tests import util


def test_extract_restore():
    client = python_pachyderm.Client()
    ops = list(client.extract())
    client.restore((python_pachyderm.RestoreRequest(op=op) for op in ops))


@util.skip_if_below_pachyderm_version(1, 8, 8)
def test_extract_pipeline_restore():
    client = python_pachyderm.Client()
    _, _, pipeline_name = util.create_test_pipeline(
        client, "test_extract_pipeline_restore"
    )
    op = client.extract_pipeline(pipeline_name)
    client.restore((python_pachyderm.RestoreRequest(op=op) for op in (op,)))


def test_inspect_cluster():
    client = python_pachyderm.Client()
    res = client.inspect_cluster()
    assert isinstance(res, python_pachyderm.ClusterInfo)
