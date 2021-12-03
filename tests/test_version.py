#!/usr/bin/env python

"""Tests for versioning-related functionality."""

import python_pachyderm
from python_pachyderm.proto.v2.version.versionpb import version_pb2


def test_remote_version():
    version_pb = python_pachyderm.Client().get_remote_version()
    assert isinstance(version_pb, version_pb2.Version)
