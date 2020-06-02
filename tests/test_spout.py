#!/usr/bin/env python

"""Tests of spout managers."""

import os
import tempfile

import grpc
import pytest

import python_pachyderm
from tests import util

def test_spout_manager():
    with tempfile.TemporaryDirectory(suffix="pachyderm") as d:
        with SpoutManager(pfs_directory=d) as spout:
            spout.add_from_bytes("foo.txt", b"bar")
            spout.add_marker_from_bytes(b"marker")
            with spout.marker() as f:
                assert f.read() == b"marker"
