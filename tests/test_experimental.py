import os
import pytest
from pathlib import Path

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
    client.mount("mount_a")
    assert open(f"mount_a/{repo_name}/file1.txt").read() == "DATA1"
    assert open(f"mount_a/{repo2_name}/file2.txt").read() == "DATA2"

    # Mount one repo
    client.mount("mount_b", [repo2_name])
    assert open(f"mount_b/{repo2_name}/file2.txt").read() == "DATA2"

    client.unmount(all_mounts=True)
    assert not os.path.exists(f"mount_a/{repo2_name}/file2.txt")
    assert not os.path.exists(f"mount_b/{repo2_name}/file2.txt")

    with client.commit(repo_name, "dev") as c3:
        client.put_file_bytes(c3, "/file3.txt", b"DATA3")

    # Mount one repo
    client.mount("mount_c", [f"{repo_name}@dev"])
    assert open(f"mount_c/{repo_name}/file3.txt").read() == "DATA3"

    client.unmount("mount_c")
    assert not os.path.exists(f"mount_c/{repo_name}/file3.txt")

    # Test runtime error
    Path("mount_d").mkdir()
    Path("mount_d/file.txt").touch()
    with pytest.raises(RuntimeError, match="must be empty to mount"):
        client.mount("mount_d")
