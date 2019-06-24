#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for the `PpsClient` class of the `python_pachyderm` package."""

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
def pps_client_with_sandbox():
    """Connect to Pachyderm before tests and reset to initial state after tests."""

    pfs_client = python_pachyderm.PfsClient()
    pps_client = python_pachyderm.PpsClient()

    pps_client.delete_all()
    pfs_client.delete_all()

    pfs_client.create_repo('test-pps-input', 'This is a test repository for PPS functionality')

    pps_client.create_pipeline(
        "test-pps-copy",
        transform=python_pachyderm.Transform(cmd=["sh"], image="alpine", stdin=["ls /pfs; cp /pfs/*.dat /pfs/out/"]),
        input=python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/*", repo="test-pps-input")),
    )

    yield pps_client, pfs_client

    pps_client.delete_all()
    pfs_client.delete_all()

def test_list_job(pps_client_with_sandbox):
    pps_client, pfs_client = pps_client_with_sandbox

    jobs = pps_client.list_job()
    assert len(jobs.job_info) == 0

    with pfs_client.commit('test-pps-input', 'master') as c:
        pfs_client.put_file_bytes(c, 'file.dat', b'DATA')

    # wait for the job to run
    pfs_client.inspect_commit(f"test-pps-input/{c.id}")

    jobs = pps_client.list_job()
    assert len(jobs.job_info) == 1

    jobs = pps_client.list_job(pipeline_name='test-pps-copy')
    assert len(jobs.job_info) == 1

    jobs = pps_client.list_job(input_commit=f"test-pps-input/{c.id}")
    assert len(jobs.job_info) == 1
