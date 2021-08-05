#!/usr/bin/env python

"""Spout tests"""
import time
import pytest
import python_pachyderm
from python_pachyderm.service import pps_proto, pfs_proto


def test_create_spout():
    client = python_pachyderm.Client()
    client.delete_all()

    client.create_pipeline(
        pipeline_name="pipeline-create-spout",
        transform=pps_proto.Transform(
            cmd=["sh"],
            image="alpine",
        ),
        spout=pps_proto.Spout(),
    )

    assert len(list(client.list_pipeline())) == 1


@pytest.mark.timeout(45)
def test_spout_commit():
    client = python_pachyderm.Client()
    client.delete_all()

    client.create_pipeline(
        pipeline_name="pipeline-spout-commit",
        transform=pps_proto.Transform(
            cmd=["bash"],
            stdin=[
                "echo 'commit time' >> file.txt",
                "pachctl put file pipeline-spout-commit@master:/file.txt -f file.txt",
            ],
        ),
        spout=pps_proto.Spout(),
    )

    c = client.subscribe_commit(
        repo_name="pipeline-spout-commit",
        branch="master",
        state=pfs_proto.FINISHED,
        origin_kind=pfs_proto.USER,
    )
    next(c)

    commit_infos = list(client.list_commit("pipeline-spout-commit"))
    assert len(commit_infos) == 2
