import os
import time

import python_pachyderm
from tests import util

"""Tests experimental features"""


def test_mount():
    client = python_pachyderm.ExperimentalClient()
    repo_name = util.create_test_repo(client, "mount", prefix="one")
    repo2_name = util.create_test_repo(client, "mount", prefix="two")

    with client.commit(repo_name, "master") as c:
        client.put_file_bytes(c, "/file1.txt", b"DATA1")
    with client.commit(repo2_name, "master") as c2:
        client.put_file_bytes(c2, "/file2.txt", b"DATA2")

    # Mount all repos
    with client.mount("mount"):
        assert open(f"mount/{repo_name}/file1.txt").read() == b"DATA1"
        assert open(f"mount/{repo2_name}/file2.txt").read() == b"DATA2"

    time.sleep(0.1)
    assert not os.path.exists(f"mount/{repo_name}/file1.txt")

    with client.commit(repo_name, "dev") as c3:
        client.put_file_bytes(c3, "/file3.txt", b"DATA3")

    # Mount one repo
    with client.mount("mount", [f"{repo_name}@dev"]):
        assert open(f"mount/{repo_name}/file3.txt").read() == b"DATA3"

    assert not os.path.exists(f"mount/{repo_name}/file3.txt")
