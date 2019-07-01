#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `PpsClient` class of the `python_pachyderm` package."""

import grpc
import time
import pytest
import random
import string

import python_pachyderm

def random_string(n):
    return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(n))

class Sandbox:
    def __init__(self, test_name):
        pfs_client = python_pachyderm.PfsClient()
        pps_client = python_pachyderm.PpsClient()

        repo_name_suffix = random_string(6)
        input_repo_name = "{}-input-{}".format(test_name, repo_name_suffix)
        pipeline_repo_name = "{}-pipeline-{}".format(test_name, repo_name_suffix)

        pfs_client.create_repo(input_repo_name, "input repo for {}".format(test_name))

        pps_client.create_pipeline(
            pipeline_repo_name,
            transform=python_pachyderm.Transform(cmd=["sh"], image="alpine", stdin=["cp /pfs/{}/*.dat /pfs/out/".format(input_repo_name)]),
            input=python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/*", repo=input_repo_name)),
            enable_stats=True,
        )

        with pfs_client.commit(input_repo_name, 'master') as commit:
            pfs_client.put_file_bytes(commit, 'file.dat', b'DATA')

        self.pps_client = pps_client
        self.pfs_client = pfs_client
        self.commit = commit
        self.input_repo_name = input_repo_name
        self.pipeline_repo_name = pipeline_repo_name

    def wait_for_job(self):
        # block until the commit is ready
        self.pfs_client.inspect_commit(self.commit, block_state=python_pachyderm.COMMIT_STATE_READY)

        # while the commit is ready, the job might not be listed on the first
        # call, so repeatedly list jobs until it's available
        start_time = time.time()
        while True:
            for job in self.pps_client.list_job():
                return job.job.id

            assert time.time() - start_time < 60.0, "timed out waiting for job"
            time.sleep(1)

def test_list_job():
    sandbox = Sandbox("list_job")
    job_id = sandbox.wait_for_job()

    jobs = list(sandbox.pps_client.list_job())
    assert len(jobs) >= 1

    jobs = list(sandbox.pps_client.list_job(pipeline_name=sandbox.pipeline_repo_name))
    assert len(jobs) >= 1

    jobs = list(sandbox.pps_client.list_job(input_commit=(sandbox.input_repo_name, sandbox.commit.id)))
    assert len(jobs) >= 1

def test_flush_job():
    sandbox = Sandbox("flush_job")
    jobs = list(sandbox.pps_client.flush_job([sandbox.commit]))
    assert len(jobs) >= 1
    print(jobs[0])

def test_inspect_job():
    sandbox = Sandbox("inspect_job")
    job_id = sandbox.wait_for_job()

    job = sandbox.pps_client.inspect_job(job_id)
    assert job.job.id == job_id

def test_stop_job():
    sandbox = Sandbox("stop_job")
    job_id = sandbox.wait_for_job()

    # This may fail if the job finished between the last call and here, so
    # ignore _Rendezvous errors.
    try:
        sandbox.pps_client.stop_job(job_id)
    except grpc._channel._Rendezvous:
        # if it failed, it should be because the job already finished
        job = sandbox.pps_client.inspect_job(job_id)
        assert job.state == python_pachyderm.JOB_SUCCESS
    else:
        # This is necessary because `StopJob` does not wait for the job to be
        # killed before returning a result.
        # TODO: remove once this is fixed:
        # https://github.com/pachyderm/pachyderm/issues/3856
        time.sleep(1)
        job = sandbox.pps_client.inspect_job(job_id)
        assert job.state == python_pachyderm.JOB_KILLED

def test_delete_job():
    sandbox = Sandbox("delete_job")
    job_id = sandbox.wait_for_job()
    orig_job_count = len(list(sandbox.pps_client.list_job()))
    sandbox.pps_client.delete_job(job_id)
    assert len(list(sandbox.pps_client.list_job())) == orig_job_count - 1

def test_datums():
    sandbox = Sandbox("datums")
    job_id = sandbox.wait_for_job()

    # flush the job so it fully finishes
    list(sandbox.pfs_client.flush_commit([(sandbox.input_repo_name, sandbox.commit.id)]))

    datums = list(sandbox.pps_client.list_datum(job_id))
    assert len(datums) == 1
    datum = sandbox.pps_client.inspect_datum(job_id, datums[0].datum_info.datum.id)
    assert datum.state == python_pachyderm.DATUM_SUCCESS

    # Just ensure this doesn't raise an exception
    sandbox.pps_client.restart_datum(job_id)

def test_inspect_pipeline():
    sandbox = Sandbox("inspect_pipeline")
    pipeline = sandbox.pps_client.inspect_pipeline(sandbox.pipeline_repo_name)
    assert pipeline.pipeline.name == sandbox.pipeline_repo_name
    pipeline = sandbox.pps_client.inspect_pipeline(sandbox.pipeline_repo_name, history=-1)
    assert pipeline.pipeline.name == sandbox.pipeline_repo_name

def test_list_pipeline():
    sandbox = Sandbox("list_pipeline")
    pipelines = sandbox.pps_client.list_pipeline()
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines.pipeline_info]
    pipelines = sandbox.pps_client.list_pipeline(history=-1)
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines.pipeline_info]

def test_delete_pipeline():
    sandbox = Sandbox("delete_pipeline")
    orig_pipeline_count = len(sandbox.pps_client.list_pipeline().pipeline_info)
    sandbox.pps_client.delete_pipeline(sandbox.pipeline_repo_name)
    assert len(sandbox.pps_client.list_pipeline().pipeline_info) == orig_pipeline_count - 1

def test_delete_all_pipelines():
    sandbox = Sandbox("delete_all_pipelines")
    sandbox.pps_client.delete_all_pipelines()
    pipelines = sandbox.pps_client.list_pipeline()
    assert len(pipelines.pipeline_info) == 0

def test_restart_pipeline():
    sandbox = Sandbox("restart_job")

    sandbox.pps_client.stop_pipeline(sandbox.pipeline_repo_name)
    pipeline = sandbox.pps_client.inspect_pipeline(sandbox.pipeline_repo_name)
    assert pipeline.stopped

    sandbox.pps_client.start_pipeline(sandbox.pipeline_repo_name)
    pipeline = sandbox.pps_client.inspect_pipeline(sandbox.pipeline_repo_name)
    assert not pipeline.stopped

def test_run_pipeline():
    sandbox = Sandbox("run_pipeline")

    # flush the job so it fully finishes
    list(sandbox.pfs_client.flush_commit([(sandbox.input_repo_name, sandbox.commit.id)]))

    # just make sure it worked
    sandbox.pps_client.run_pipeline(sandbox.pipeline_repo_name)

def test_get_pipeline_logs():
    sandbox = Sandbox("get_pipeline_logs")
    job_id = sandbox.wait_for_job()

    # Wait for the job to complete
    list(sandbox.pps_client.flush_job([sandbox.commit]))

    # Just make sure these spit out some logs
    logs = sandbox.pps_client.get_pipeline_logs(sandbox.pipeline_repo_name)
    assert next(logs) is not None

    logs = sandbox.pps_client.get_pipeline_logs(sandbox.pipeline_repo_name, master=True)
    assert next(logs) is not None

def test_get_job_logs():
    sandbox = Sandbox("get_logs_logs")
    job_id = sandbox.wait_for_job()

    # Wait for the job to complete
    list(sandbox.pps_client.flush_job([sandbox.commit]))

    # Just make sure these spit out some logs
    logs = sandbox.pps_client.get_job_logs(job_id)
    assert next(logs) is not None
