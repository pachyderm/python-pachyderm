#!/usr/bin/env python

"""Tests for versioning-related functionality."""

import json
from os.path import join
from os.path import dirname

import python_pachyderm
from python_pachyderm.experimental.service import version_proto


def test_local_version():
    with open(join(dirname(dirname(dirname(__file__))), "version.json"), "r") as f:
        j = json.load(f)
        version = j["python-pachyderm"]
    assert python_pachyderm.__version__ == version


def test_remote_version():
    version_pb = python_pachyderm.experimental.Client().get_remote_version()
    assert isinstance(version_pb, version_proto.Version)
