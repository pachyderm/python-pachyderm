#!/usr/bin/env python

"""Tests of utility functions."""

import os
import tempfile

import grpc

import python_pachyderm
from tests import util

def test_put_files():
    client = python_pachyderm.Client()
    repo_name = util.create_test_repo(client, "put_files")

    with tempfile.TemporaryDirectory(suffix="python_pachyderm") as d:
        # create a temporary directory with these files:
        # 0.txt  1.txt  2.txt  3.txt  4.txt  0/0.txt  1/1.txt  2/2.txt
        # 3/3.txt  4/4.txt
        for i in range(5):
            os.makedirs(os.path.join(d, str(i)))

        for j in range(5):
            with open(os.path.join(d, "{}.txt".format(j)), "w") as f:
                f.write(str(j))
            with open(os.path.join(d, str(j), "{}.txt".format(j)), "w") as f:
                f.write(str(j))

        # add the files under both `/` and `/sub` (the latter redundantly to
        # test both for correct path handling and the ability to put files
        # that already exist)
        commit = "{}/master".format(repo_name)
        python_pachyderm.put_files(client, d, commit, "/")
        python_pachyderm.put_files(client, d, commit, "/sub")
        python_pachyderm.put_files(client, d, commit, "/sub/")

    expected = set(["/", "/sub"])
    for i in range(5):
        expected.add("/{}".format(i))
        expected.add("/{}.txt".format(i))
        expected.add("/{}/{}.txt".format(i, i))
        expected.add("/sub/{}".format(i))
        expected.add("/sub/{}.txt".format(i))
        expected.add("/sub/{}/{}.txt".format(i, i))

    for fi in client.walk_file(commit, "/"):
        path = fi.file.path
        assert path in expected, "unexpected path: {}".format(path)
        expected.remove(path)

    for path in expected:
        assert False, "expected path not found: {}".format(path)

def test_create_python_pipeline():
    raise NotImplementedError
