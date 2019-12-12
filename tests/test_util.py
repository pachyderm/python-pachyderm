#!/usr/bin/env python

"""Tests of utility functions."""

import os
import tempfile

import grpc
import pytest

import python_pachyderm
from tests import util

TEST_MAIN_SOURCE = """
from shutil import copyfile
print("copying")
copyfile("/pfs/{}/file.dat", "/pfs/out/file.dat")
"""

TEST_REQUIREMENTS_SOURCE = """
# WCGW?
leftpad==0.1.2
"""

def check_expected_files(client, commit, expected):
    for fi in client.walk_file(commit, "/"):
        path = fi.file.path
        assert path in expected, "unexpected path: {}".format(path)
        expected.remove(path)

    for path in expected:
        assert False, "expected path not found: {}".format(path)

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

    check_expected_files(client, commit, expected)

def test_create_python_pipeline():
    client = python_pachyderm.Client()
    repo_name = util.create_test_repo(client, "create_python_pipeline")
    pfs_input = python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/", repo=repo_name))
    pipeline_name = util.test_repo_name("create_python_pipeline", prefix="pipeline")

    # create some sample data
    with client.commit(repo_name, "master") as commit:
        client.put_file_bytes(commit, 'file.dat', b'DATA')

    # 1) create a pipeline from a file that does not exist - should
    # fail
    with pytest.raises(Exception):
        python_pachyderm.create_python_pipeline(client, "./foobar2000", pfs_input)

    # 2) create a pipeline from an empty directory - should fail
    with pytest.raises(Exception):
        with tempfile.TemporaryDirectory(suffix="python_pachyderm") as d:
            python_pachyderm.create_python_pipeline(client, d, pfs_input)

    # 3) create a pipeline from a directory with a main.py and
    # requirements.txt
    with tempfile.TemporaryDirectory(suffix="python_pachyderm") as d:
        with open(os.path.join(d, "main.py"), "w") as f:
            f.write(TEST_MAIN_SOURCE.format(repo_name))
        with open(os.path.join(d, "requirements.txt"), "w") as f:
            f.write(TEST_REQUIREMENTS_SOURCE)

        python_pachyderm.create_python_pipeline(client, d, pfs_input, pipeline_name=pipeline_name)

    list(client.flush_commit([c.commit for c in client.list_commit(pipeline_name)]))

    check_expected_files(client, "{}_source/master".format(pipeline_name), set([
        "/",
        "/main.py",
        "/requirements.txt",
        "/build.sh",
        "/run.sh",
    ]))

    check_expected_files(client, "{}_build/master".format(pipeline_name), set([
        "/",
        "/leftpad-0.1.2-py3-none-any.whl",
    ]))

    check_expected_files(client, "{}/master".format(pipeline_name), set([
        "/",
        "/file.dat",
    ]))

    file = list(client.get_file('{}/master'.format(pipeline_name), 'file.dat'))
    assert file == [b'DATA']

    # 4) create a pipeline from a directory without a requirements.txt
    with tempfile.TemporaryDirectory(suffix="python_pachyderm") as d:
        with open(os.path.join(d, "main.py"), "w") as f:
            f.write(TEST_MAIN_SOURCE.format(repo_name))

        python_pachyderm.create_python_pipeline(
            client, d, pfs_input,
            pipeline_name=pipeline_name,
            #pipeline_kwargs=dict(reprocess=True),
            update=True,
        )

    list(client.flush_commit([c.commit for c in client.list_commit(pipeline_name)]))

    check_expected_files(client, "{}_source/master".format(pipeline_name), set([
        "/",
        "/main.py",
        "/run.sh",
    ]))

    check_expected_files(client, "{}/master".format(pipeline_name), set([
        "/",
        "/file.dat",
    ]))

    file = list(client.get_file('{}/master'.format(pipeline_name), 'file.dat'))
    assert file == [b'DATA']

    # 5) create a pipeline from just a single file
    with tempfile.NamedTemporaryFile(suffix="python_pachyderm", mode="w") as f:
        f.write(TEST_MAIN_SOURCE.format(repo_name))
        f.flush()

        python_pachyderm.create_python_pipeline(
            client, f.name, pfs_input,
            pipeline_name=pipeline_name,
            #pipeline_kwargs=dict(reprocess=True),
            update=True,
        )

    list(client.flush_commit([c.commit for c in client.list_commit(pipeline_name)]))

    check_expected_files(client, "{}_source/master".format(pipeline_name), set([
        "/",
        "/main.py",
        "/run.sh",
    ]))

    check_expected_files(client, "{}/master".format(pipeline_name), set([
        "/",
        "/file.dat",
    ]))

    file = list(client.get_file('{}/master'.format(pipeline_name), 'file.dat'))
    assert file == [b'DATA']
