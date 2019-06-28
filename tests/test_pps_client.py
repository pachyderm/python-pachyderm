#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `PpsClient` class of the `python_pachyderm` package."""

import grpc
import time
import pytest

import python_pachyderm


@pytest.fixture(scope='function')
def pps_client():
    """Connect to Pachyderm before tests and reset to initial state after tests."""
    client = python_pachyderm.PpsClient()
    client.delete_all()
    yield client 
    client.delete_all()


@pytest.fixture(scope='function')
def clients_with_sandbox():
    """Connect to Pachyderm before tests and reset to initial state after tests."""

    pfs_client = python_pachyderm.PfsClient()
    pps_client = python_pachyderm.PpsClient()

    pps_client.delete_all()
    pfs_client.delete_all()

    pfs_client.create_repo('test-pps-input', 'This is a test repository for PPS functionality')

    pps_client.create_pipeline(
        "test-pps-copy",
        transform=python_pachyderm.Transform(cmd=["sh"], image="alpine", stdin=["cp /pfs/test-pps-input/*.dat /pfs/out/"]),
        input=python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/*", repo="test-pps-input")),
        enable_stats=True,
    )

    with pfs_client.commit('test-pps-input', 'master') as commit:
        pfs_client.put_file_bytes(commit, 'file.dat', b'DATA')

    yield pps_client, pfs_client, commit

    pps_client.delete_all()
    pfs_client.delete_all()

def wait_for_job(pps_client, pfs_client, commit):
    # block until the commit is ready
    pfs_client.inspect_commit(commit, block_state=python_pachyderm.COMMIT_STATE_READY)

    # while the commit is ready, the job might not be listed on the first
    # call, so repeatedly list jobs until it's available
    start_time = time.time()
    while True:
        for job in pps_client.list_job():
            return job.job.id

        assert time.time() - start_time < 60.0, "timed out waiting for job"
        time.sleep(1)

def test_list_job(clients_with_sandbox):
    pps_client, pfs_client, commit = clients_with_sandbox
    job_id = wait_for_job(pps_client, pfs_client, commit)

    jobs = list(pps_client.list_job())
    assert len(jobs) == 1

    jobs = list(pps_client.list_job(pipeline_name='test-pps-copy'))
    assert len(jobs) == 1

    jobs = list(pps_client.list_job(input_commit="test-pps-input/{}".format(commit.id)))
    assert len(jobs) == 1

def test_inspect_job(clients_with_sandbox):
    pps_client, pfs_client, commit = clients_with_sandbox
    job_id = wait_for_job(pps_client, pfs_client, commit)

    job = pps_client.inspect_job(job_id)
    assert job.job.id == job_id

def test_stop_job(clients_with_sandbox):
    pps_client, pfs_client, commit = clients_with_sandbox
    job_id = wait_for_job(pps_client, pfs_client, commit)

    # This may fail if the job finished between the last call and here, so
    # ignore _Rendezvous errors.
    try:
        pps_client.stop_job(job_id)
    except grpc._channel._Rendezvous:
        # if it failed, it should be because the job already finished
        job = pps_client.inspect_job(job_id)
        assert job.state == python_pachyderm.JOB_SUCCESS
    else:
        # This is necessary because `StopJob` does not wait for the job to be
        # killed before returning a result.
        # TODO: remove once this is fixed:
        # https://github.com/pachyderm/pachyderm/issues/3856
        time.sleep(1)
        job = pps_client.inspect_job(job_id)
        assert job.state == python_pachyderm.JOB_KILLED

def test_delete_job(clients_with_sandbox):
    pps_client, pfs_client, commit = clients_with_sandbox
    job_id = wait_for_job(pps_client, pfs_client, commit)

    pps_client.delete_job(job_id)
    jobs = list(pps_client.list_job())
    assert len(jobs) == 0

def test_datums(clients_with_sandbox):
    pps_client, pfs_client, commit = clients_with_sandbox
    job_id = wait_for_job(pps_client, pfs_client, commit)

    # flush the job so it fully finishes
    list(pfs_client.flush_commit(["test-pps-input/{}".format(commit.id)]))

    datums = list(pps_client.list_datum(job_id))
    assert len(datums) == 1
    datum = pps_client.inspect_datum(job_id, datums[0].datum_info.datum.id)
    assert datum.state == python_pachyderm.DATUM_SUCCESS

    # Just ensure this doesn't raise an exception
    pps_client.restart_datum(job_id)

def test_inspect_pipeline(clients_with_sandbox):
    pps_client, _, _ = clients_with_sandbox
    pipeline = pps_client.inspect_pipeline('test-pps-copy')
    assert pipeline.pipeline.name == 'test-pps-copy'

def test_list_pipeline(clients_with_sandbox):
    pps_client, _, _ = clients_with_sandbox
    pipelines = pps_client.list_pipeline()
    assert len(pipelines.pipeline_info) == 1
    assert pipelines.pipeline_info[0].pipeline.name == 'test-pps-copy'

def test_delete_pipeline(clients_with_sandbox):
    pps_client, _, _ = clients_with_sandbox
    pps_client.delete_pipeline('test-pps-copy')
    pipelines = pps_client.list_pipeline()
    assert len(pipelines.pipeline_info) == 0

def test_delete_all_pipelines(clients_with_sandbox):
    pps_client, _, _ = clients_with_sandbox
    pps_client.delete_all_pipelines()
    pipelines = pps_client.list_pipeline()
    assert len(pipelines.pipeline_info) == 0

def test_restart_pipeline(clients_with_sandbox):
    pps_client, _, _ = clients_with_sandbox

    pps_client.stop_pipeline('test-pps-copy')
    pipeline = pps_client.inspect_pipeline('test-pps-copy')
    assert pipeline.stopped

    pps_client.start_pipeline('test-pps-copy')
    pipeline = pps_client.inspect_pipeline('test-pps-copy')
    assert not pipeline.stopped

# TODO: re-enable test
# def test_get_logs(clients_with_sandbox):
#     pps_client, pfs_client, commit = clients_with_sandbox
#     job_id = wait_for_job(pps_client, pfs_client, commit)

#     # Just make sure these spit out some logs
#     logs = pps_client.get_logs(pipeline_name='test-pps-copy')
#     assert next(logs) is not None

#     logs = pps_client.get_logs(job_id=job_id)
#     assert next(logs) is not None

#     logs = pps_client.get_logs(pipeline_name='test-pps-copy', job_id=job_id)
#     assert next(logs) is not None

#     logs = pps_client.get_logs(pipeline_name='test-pps-copy', master=True)
#     assert next(logs) is not None

def test_garbage_collect(pps_client):
    # just make sure this doesn't error
    pps_client.garbage_collect()
