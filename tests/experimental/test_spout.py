#!/usr/bin/env python

"""Spout tests"""
import pytest

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.pfs import CommitState, OriginKind
from python_pachyderm.experimental.mixin.pps import Transform, Spout


def test_create_spout():
    client = ExperimentalClient()
    client.delete_all()

    client.pps.create_pipeline(
        pipeline_name="pipeline-create-spout",
        transform=Transform(cmd=["sh"], image="alpine"),
        spout=Spout(),
    )

    assert len(list(client.pps.list_pipeline())) == 1


@pytest.mark.timeout(60)
def test_spout_commit():
    client = ExperimentalClient()
    client.delete_all()

    client.pps.create_pipeline(
        pipeline_name="pipeline-spout-commit",
        transform=Transform(
            cmd=["bash"],
            stdin=[
                "echo 'commit time' >> file.txt",
                "pachctl put file pipeline-spout-commit@master:/file.txt -f file.txt",
            ],
        ),
        spout=Spout(),
    )

    # This leaves the stream opens and needs to be manually closed.
    # Should probably be a context manager
    c = client.pfs.subscribe_commit(
        repo_name="pipeline-spout-commit",
        branch="master",
        state=CommitState.FINISHED,
        origin_kind=OriginKind.USER,
    )
    next(c)
    del c

    commit_infos = list(client.pfs.list_commit("pipeline-spout-commit"))
    assert len(commit_infos) == 1
