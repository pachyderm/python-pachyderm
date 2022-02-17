#!/usr/bin/env python

"""Tests for PPS-related functionality."""
import os
from typing import Iterable, Optional
from uuid import uuid4

import grpclib
import pytest

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.pps import DatumState, JobInfo, JobState


def test_pipelines(client: ExperimentalClient, pipeline: str, repo: str):
    """Test the full functionality of pipelines."""
    # Assert that the test pipeline is listed
    pipeline_info = client.pps.list_pipeline()
    assert any(info for info in pipeline_info if info.pipeline.name == pipeline)

    # Test inspecting pipelines.
    pipeline_info = client.pps.inspect_pipeline(pipeline)
    assert any(info for info in pipeline_info if info.pipeline.name == pipeline)

    client.pps.stop_pipeline(pipeline)
    assert next(client.pps.inspect_pipeline(pipeline)).stopped

    client.pps.start_pipeline(pipeline)
    assert not next(client.pps.inspect_pipeline(pipeline)).stopped

    with pytest.raises(grpclib.GRPCError, match=r"pipeline.*have a cron input"):
        client.pps.run_cron(pipeline)

    # Just make sure these spit out some logs
    assert client.pps.get_pipeline_logs(pipeline) is not None
    assert client.pps.get_pipeline_logs(pipeline, master=True) is not None

    client.pps.delete_pipeline(pipeline)
    pipeline_info = client.pps.list_pipeline()
    assert not any(info for info in pipeline_info if info.pipeline.name == pipeline)


def test_jobs(client: ExperimentalClient, pipeline: str, repo: str):
    """Test the PPS functionality relating to jobs"""

    def contains_pipeline_job(
        job_info_iterable: Iterable[JobInfo],
        pipeline_name: str,
        job_commit: Optional[str] = None,
    ) -> bool:
        for _info in job_info_iterable:
            if _info.job.pipeline.name == pipeline_name:
                return True and (job_commit is None or _info.job.id == job_commit)
        return False

    # Assert that jobs are listed as expected.
    #   Listing all jobs returns Iterable[JobSetInfo]
    job_sets = client.pps.list_job()
    assert any(js for js in job_sets if contains_pipeline_job(js.jobs, pipeline))

    #   Listing all jobs filtered by pipeline name returns Iterable[JobInfo]
    job_infos = list(client.pps.list_job(pipeline_name=pipeline))
    assert contains_pipeline_job(job_infos, pipeline)
    assert len(job_infos) == 1
    commit = job_infos[0].output_commit

    job_infos = list(client.pps.list_job(pipeline_name=pipeline, input_commit=commit))
    assert contains_pipeline_job(job_infos, pipeline, commit.id)

    # Assert that jobs can be inspected from commit id.
    job_info = next(client.pps.inspect_job(commit.id, pipeline))
    assert job_info.job.id == commit.id
    assert job_info.job.pipeline.name == pipeline

    # Test stopping a job.
    reason = "stopped for testing"
    client.pps.stop_job(commit.id, pipeline, reason=reason)
    job_info = next(client.pps.inspect_job(commit.id, pipeline))
    assert job_info.state == JobState.JOB_KILLED
    assert job_info.reason == reason

    # Test deleting a job.
    client.pps.delete_job(commit.id, pipeline)
    job_infos = list(client.pps.list_job(pipeline_name=pipeline, input_commit=commit))
    assert not contains_pipeline_job(job_infos, pipeline, commit.id)


def test_datums(client: ExperimentalClient, pipeline: str, repo: str):
    with client.pfs.commit(repo, "master") as commit:
        client.pfs.put_file_bytes(commit, f"/{commit.id}.dat", os.urandom(1024))

    # We must wait for the commit to finish for the datum to be created.
    pipeline_commit_info = client.pfs.wait_commit((pipeline, commit.id))[0]
    job_id = pipeline_commit_info.commit.id
    datum_info = next(client.pps.list_datum(pipeline, job_id))
    assert datum_info.state == DatumState.SUCCESS

    datum_info = client.pps.inspect_datum(pipeline, job_id, datum_info.datum.id)
    assert datum_info.state == DatumState.SUCCESS

    assert next(client.pps.get_job_logs(pipeline, job_id)) is not None

    error_message_match = (
        rf"datum matching filter \[.*\] could not be found for job ID {job_id}"
    )
    with pytest.raises(grpclib.GRPCError, match=error_message_match):
        client.pps.restart_datum(pipeline, job_id)


def test_secrets(client: ExperimentalClient):
    secret_name = uuid4().hex
    data = {"mykey": "my-value"}
    client.pps.create_secret(secret_name, data)

    secret_info = client.pps.inspect_secret(secret_name)
    assert secret_info.secret.name == secret_name
    assert any(
        info for info in client.pps.list_secret() if info.secret.name == secret_name
    )

    client.pps.delete_secret(secret_name)
    with pytest.raises(grpclib.GRPCError):
        client.pps.inspect_secret(secret_name)
    assert len(client.pps.list_secret()) == 0
