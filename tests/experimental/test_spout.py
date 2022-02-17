#!/usr/bin/env python

"""Spout tests"""
import pytest

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.pfs import CommitState, OriginKind
from python_pachyderm.experimental.mixin.pps import Transform, Spout


@pytest.mark.timeout(30)
def test_spout_commit(client: ExperimentalClient, repo: str):
    pipeline_name = "pipeline-create-spout"
    client.pps.delete_pipeline(pipeline_name, force=True)
    try:
        client.pps.create_pipeline(
            pipeline_name=pipeline_name,
            transform=Transform(
                cmd=["bash"],
                stdin=[
                    "echo 'commit time' >> file.txt",
                    f"pachctl put file {pipeline_name}@master:/file.txt -f file.txt",
                ],
            ),
            spout=Spout(),
        )

        # This leaves the stream opens and needs to be manually closed.
        # Should probably be a context manager
        c = client.pfs.subscribe_commit(
            repo_name=pipeline_name,
            branch="master",
            state=CommitState.FINISHED,
            origin_kind=OriginKind.USER,
        )
        next(c)
        del c

        commit_infos = list(client.pfs.list_commit(pipeline_name))
        assert len(commit_infos) == 1

    finally:
        client.pps.delete_pipeline(pipeline_name, force=True)
