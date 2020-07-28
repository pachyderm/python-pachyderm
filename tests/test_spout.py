#!/usr/bin/env python

"""Tests of spout managers."""

import tempfile

import python_pachyderm


def test_spout_manager():
    with tempfile.TemporaryDirectory(suffix="pachyderm") as d:
        manager = python_pachyderm.SpoutManager(pfs_directory=d, marker_filename="marker")

        with manager.commit() as commit:
            commit.add_from_bytes("foo1.txt", b"bar1")
            commit.add_marker_from_bytes(b"marker1")
        with manager.commit() as commit:
            commit.add_from_bytes("foo2.txt", b"bar2")
            commit.add_marker_from_bytes(b"marker2")
