#!/usr/bin/env python

"""Tests of spout managers."""

import tempfile

import pytest
import python_pachyderm


def test_spout_manager():
    with tempfile.TemporaryDirectory(suffix="pachyderm") as d:
        manager = python_pachyderm.SpoutManager(pfs_directory=d, marker_filename="marker")

        with manager.commit() as commit:
            commit.put_file_from_bytes("foo1.txt", b"bar1")
            commit.put_marker_from_bytes(b"marker1")
        with manager.commit() as commit:
            commit.put_file_from_bytes("foo2.txt", b"bar2")
            commit.put_marker_from_bytes(b"marker2")

def test_spout_manager_nested_commits():
    with tempfile.TemporaryDirectory(suffix="pachyderm") as d:
        manager = python_pachyderm.SpoutManager(pfs_directory=d, marker_filename="marker")

        with manager.commit() as commit:
            commit.put_file_from_bytes("foo1.txt", b"bar1")
            commit.put_marker_from_bytes(b"marker1")

            with pytest.raises(Exception):
                with manager.commit() as commit:
                    pass

def test_spout_manager_commit_state():
    with tempfile.TemporaryDirectory(suffix="pachyderm") as d:
        manager = python_pachyderm.SpoutManager(pfs_directory=d, marker_filename="marker")

        with pytest.raises(Exception):
            with manager.commit() as commit:
                raise Exception()

        assert manager._has_open_commit
