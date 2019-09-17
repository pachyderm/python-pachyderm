#!/usr/bin/env python

"""Tests for the `get_remote_version` function of the `python_pachyderm` package."""

from python_pachyderm import version


def test_pfs_client_init_with_default_host_port():
    # GIVEN a Pachyderm deployment
    # WHEN a client requests the remote version of a pachyderm deployment
    version_pb = version.get_remote_version()
    # THEN remote version should be a version object
    assert isinstance(version_pb, version.Version)
