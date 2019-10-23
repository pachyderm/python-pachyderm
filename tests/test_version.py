#!/usr/bin/env python

"""Tests for versioning-related functionality."""

import python_pachyderm


def test_remote_version():
    version_pb = python_pachyderm.Client().get_remote_version()
    assert isinstance(version_pb, python_pachyderm.Version)
