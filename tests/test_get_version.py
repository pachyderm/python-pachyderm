#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `get_remote_version` function of the `python_pachyderm` package."""
import python_pachyderm

def test_pfs_client_init_with_default_host_port():
    # GIVEN a Pachyderm deployment
    # WHEN a client requests the remote version of a pachyderm deployment
    version_pb = python_pachyderm.get_remote_version()
    remote_version = "{}.{}.{}\n".format(
            version_pb.major, version_pb.minor, version_pb.micro)
    # THEN remote version should match the pachctl version which deployed it
    with open("VERSION") as version_file:
        local_version = version_file.read()
    assert remote_version == local_version
