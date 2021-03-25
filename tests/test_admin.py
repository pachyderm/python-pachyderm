#!/usr/bin/env python

"""Tests admin-related functionality"""

import pytest

import python_pachyderm
from tests import util


def test_inspect_cluster():
    client = python_pachyderm.Client()
    res = client.inspect_cluster()
    assert isinstance(res, python_pachyderm.ClusterInfo)
