#!/usr/bin/env python

"""Tests for versioning-related functionality."""

import json
from os.path import join
from os.path import dirname

import python_pachyderm


def test_local_version():
    with open(join(dirname(dirname(__file__)), "version.json"), "r") as f:
        j = json.load(f)
        version = j["python-pachyderm"]
    assert python_pachyderm.__version__ == version


def test_remote_version():
    version_pb = python_pachyderm.Client().get_remote_version()
    assert isinstance(version_pb, python_pachyderm.Version)
