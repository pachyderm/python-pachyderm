#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `get_remote_version` function of the `python_pachyderm` package."""
import python_pachyderm
from python_pachyderm.client.version.versionpb.version_pb2 import Version


def test_pfs_client_init_with_default_host_port():
    # GIVEN a Pachyderm deployment
    # WHEN a client requests the remote version of a pachyderm deployment
    version_pb = python_pachyderm.get_remote_version()
    # THEN remote version should be a version object
    assert isinstance(version_pb, Version)
