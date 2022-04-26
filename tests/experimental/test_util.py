#!/usr/bin/env python

"""Tests of utility functions."""

import os
import json
import tempfile
from pathlib import Path
from shutil import which

import pytest

import python_pachyderm
from python_pachyderm.experimental.service import pps_proto
from python_pachyderm.experimental.util import check_pachctl
from tests import util

# bp_to_pb: PfsInput -> PFSInput

# script that copies a file using just stdlibs
TEST_STDLIB_SOURCE = """
from shutil import copyfile
print("copying")
copyfile("/pfs/{}/file.dat", "/pfs/out/file.dat")
"""

# script that copies a file with padding and colorized output, using
# third-party libraries (defined in `TEST_REQUIREMENTS_SOURCE`.)
TEST_LIB_SOURCE = """
from termcolor import cprint
from leftpad import left_pad

cprint('copying', 'green')

with open('/pfs/{}/file.dat', 'r') as f:
    contents = f.read()

with open('/pfs/out/file.dat', 'w') as f:
    f.write(left_pad(contents, 5))
"""

TEST_REQUIREMENTS_SOURCE = """
# WCGW?
leftpad==0.1.2
termcolor==1.1.0
"""

TEST_PIPELINE_SPEC = """
{
  "pipeline": {
    "name": "foobar"
  },
  "description": "A pipeline that performs image edge detection by using the OpenCV library.",
  "input": {
    "pfs": {
      "glob": "/*",
      "repo": "images"
    }
  },
  "transform": {
    "cmd": [ "python3", "/edges.py" ],
    "image": "pachyderm/opencv"
  }
}
"""


def check_expected_files(client: python_pachyderm.Client, commit, expected):
    for fi in client.walk_file(commit, "/"):
        path = fi.file.path
        assert path in expected, "unexpected path: {}".format(path)
        expected.remove(path)

    for path in expected:
        assert False, "expected path not found: {}".format(path)


def test_put_files():
    client = python_pachyderm.experimental.Client()
    client.delete_all()
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
        commit = (repo_name, "master")
        python_pachyderm.put_files(client, d, commit, "/")
        python_pachyderm.put_files(client, d, commit, "/sub")
        python_pachyderm.put_files(client, d, commit, "/sub/")

    expected = set(["/", "/sub/"])
    for i in range(5):
        expected.add("/{}/".format(i))
        expected.add("/{}.txt".format(i))
        expected.add("/{}/{}.txt".format(i, i))
        expected.add("/sub/{}/".format(i))
        expected.add("/sub/{}.txt".format(i))
        expected.add("/sub/{}/{}.txt".format(i, i))

    check_expected_files(client, commit, expected)


def test_put_files_single_file():
    client = python_pachyderm.experimental.Client()
    client.delete_all()
    repo_name = util.create_test_repo(client, "put_files_single_file")

    with tempfile.NamedTemporaryFile() as f:
        f.write(b"abcd")
        f.flush()
        commit = (repo_name, "master")
        python_pachyderm.put_files(client, f.name, commit, "/f1.txt")
        python_pachyderm.put_files(client, f.name, commit, "/f/f1")

    expected = set(["/", "/f1.txt", "/f/", "/f/f1"])
    check_expected_files(client, commit, expected)


def test_parse_json_pipeline_spec():
    req = python_pachyderm.experimental.parse_json_pipeline_spec(TEST_PIPELINE_SPEC)
    check_pipeline_spec(req)


def test_parse_dict_pipeline_spec():
    req = python_pachyderm.experimental.parse_dict_pipeline_spec(
        json.loads(TEST_PIPELINE_SPEC)
    )
    check_pipeline_spec(req)


def check_pipeline_spec(req):
    assert req == pps_proto.CreatePipelineRequest(
        pipeline=pps_proto.Pipeline(name="foobar"),
        description="A pipeline that performs image edge detection by using the OpenCV library.",
        input=pps_proto.Input(
            pfs=pps_proto.PfsInput(glob="/*", repo="images"),
        ),
        transform=pps_proto.Transform(
            cmd=["python3", "/edges.py"],
            image="pachyderm/opencv",
        ),
    )


def test_check_pachctl(tmp_path: Path):
    # Sanity check that pachctl is detected.
    assert check_pachctl() is None

    path_env = os.environ["PATH"]
    try:
        false_executable = which("false")

        # Temporary overwrite PATH (hiding pachctl)
        # This should cause check_pachctl to error.
        os.environ["PATH"] = str(tmp_path)
        with pytest.raises(FileNotFoundError):
            check_pachctl()

        # Create a pachctl that errors (symlink to `false`)
        # This should cause check_pachctl to error.
        bad_pachctl = Path(tmp_path, "pachctl")
        bad_pachctl.symlink_to(false_executable)
        with pytest.raises(RuntimeError):
            check_pachctl()
    finally:
        os.environ["PATH"] = path_env
