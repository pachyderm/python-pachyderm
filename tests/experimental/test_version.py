#!/usr/bin/env python

"""Tests for versioning-related functionality."""

import json
from os.path import join
from os.path import dirname

import python_pachyderm
from python_pachyderm.experimental.service import version_proto


def test_remote_version():
    version_pb = python_pachyderm.experimental.Client().version.get_remote_version()
    assert isinstance(version_pb, version_proto.Version)
