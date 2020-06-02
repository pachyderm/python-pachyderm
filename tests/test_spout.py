#!/usr/bin/env python

"""Tests of spout managers."""

import tempfile

import python_pachyderm


def test_spout_manager_write():
    with tempfile.TemporaryDirectory(suffix="pachyderm") as d:
        with python_pachyderm.SpoutManager(pfs_directory=d) as spout:
            spout.add_from_bytes("foo.txt", b"bar")

def test_spout_manager_marker():
    with tempfile.TemporaryDirectory(suffix="pachyderm",) as d:
        with python_pachyderm.SpoutManager(pfs_directory=d, marker_filename="marker") as spout:
            spout.add_marker_from_bytes(b"marker")
            with spout.marker() as f:
                assert f.read() == b"marker"
