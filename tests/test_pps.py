#!/usr/bin/env python

"""Tests for PPS-related functionality."""

import time

import pytest

import python_pachyderm
from python_pachyderm.service import pps_proto
from tests import util


class Sandbox:
    def __init__(self, test_name):
        client = python_pachyderm.Client()
        client.delete_all()
        commit, input_repo_name, pipeline_repo_name = util.create_test_pipeline(
            client, test_name
        )
        self.client = client
        self.commit = commit
        self.input_repo_name = input_repo_name
        self.pipeline_repo_name = pipeline_repo_name

    def wait(self):
        return self.client.wait_commit(self.commit.id)[0].commit.id


def test_list_subjob():
    sandbox = Sandbox("list_subjob")
    sandbox.wait()

    jobs = list(sandbox.client.list_job())
    assert len(jobs) >= 1

    jobs = list(sandbox.client.list_job(pipeline_name=sandbox.pipeline_repo_name))
    assert len(jobs) >= 1

    jobs = list(
        sandbox.client.list_job(
            pipeline_name=sandbox.pipeline_repo_name,
            input_commit=(sandbox.input_repo_name, sandbox.commit.id),
        )
    )
    assert len(jobs) >= 1


def test_list_job():
    sandbox = Sandbox("list_job1")
    sandbox.wait()
    client = sandbox.client

    commit, _, _ = util.create_test_pipeline(client, "list_job2")
    client.wait_commit(commit.id)

    jobs = list(client.list_job())
    assert len(jobs) == 4


def test_inspect_subjob():
    sandbox = Sandbox("inspect_subjob")
    job_id = sandbox.wait()

    job_info = list(sandbox.client.inspect_job(job_id, sandbox.pipeline_repo_name))
    assert job_info[0].job.id == job_id


def test_inspect_job():
    sandbox = Sandbox("inspect_job1")
    sandbox.wait()
    client = sandbox.client

    commit, _, _ = util.create_test_pipeline(client, "inspect_job2")
    client.wait_commit(commit.id)

    jobs = list(client.list_job())
    job1 = list(client.inspect_job(job_id=jobs[0].job_set.id))

    assert job1[0].job.id == commit.id
    assert len(jobs) == 4


def test_stop_job():
    sandbox = Sandbox("stop_job")
    pipeline_name = sandbox.pipeline_repo_name
    job_id = sandbox.wait()

    sandbox.client.stop_job(job_id, pipeline_name)
    # This is necessary because `StopJob` does not wait for the job to be
    # killed before returning a result.
    # TODO: remove once this is fixed:
    # https://github.com/pachyderm/pachyderm/issues/3856
    time.sleep(1)
    job_info = list(sandbox.client.inspect_job(job_id, pipeline_name))
    # We race to stop the job before it finishes - if we lose the race, it will
    # be in state JOB_SUCCESS
    assert job_info[0].state in [
        pps_proto.JobState.JOB_KILLED,
        pps_proto.JobState.JOB_SUCCESS,
    ]


def test_delete_job():
    sandbox = Sandbox("delete_job")
    job_id = sandbox.wait()
    orig_job_count = len(list(sandbox.client.list_job()))
    sandbox.client.delete_job(job_id, sandbox.pipeline_repo_name)
    jobs = len(list(sandbox.client.list_job()))
    assert jobs == orig_job_count - 1


def test_datums():
    sandbox = Sandbox("datums")
    pipeline_name = sandbox.pipeline_repo_name
    job_id = sandbox.wait()

    # flush the job so it fully finishes
    list(sandbox.client.wait_commit(sandbox.commit.id))

    datums = list(sandbox.client.list_datum(pipeline_name, job_id))
    assert len(datums) == 1
    datum = sandbox.client.inspect_datum(pipeline_name, job_id, datums[0].datum.id)
    assert datum.state == pps_proto.DatumState.SUCCESS

    with pytest.raises(
        python_pachyderm.RpcError,
        match=r"datum matching filter \[.*\] could not be found for job ID {}".format(
            job_id
        ),
    ):
        sandbox.client.restart_datum(pipeline_name, job_id)


def test_inspect_pipeline():
    sandbox = Sandbox("inspect_pipeline")
    pipeline = list(sandbox.client.inspect_pipeline(sandbox.pipeline_repo_name))[0]
    assert pipeline.pipeline.name == sandbox.pipeline_repo_name
    pipelines = list(
        sandbox.client.inspect_pipeline(sandbox.pipeline_repo_name, history=-1)
    )
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines]


def test_list_pipeline():
    sandbox = Sandbox("list_pipeline")
    pipelines = list(sandbox.client.list_pipeline())
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines]
    pipelines = list(sandbox.client.list_pipeline(history=-1))
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines]


def test_delete_pipeline():
    sandbox = Sandbox("delete_pipeline")
    orig_pipeline_count = len(list(sandbox.client.list_pipeline()))
    sandbox.client.delete_pipeline(sandbox.pipeline_repo_name)
    assert len(list(sandbox.client.list_pipeline())) == orig_pipeline_count - 1


def test_delete_all_pipelines():
    sandbox = Sandbox("delete_all_pipelines")
    sandbox.client.delete_all_pipelines()
    pipelines = list(sandbox.client.list_pipeline())
    assert len(pipelines) == 0


def test_restart_pipeline():
    sandbox = Sandbox("restart_job")

    sandbox.client.stop_pipeline(sandbox.pipeline_repo_name)
    pipeline = list(sandbox.client.inspect_pipeline(sandbox.pipeline_repo_name))[0]
    assert pipeline.stopped

    sandbox.client.start_pipeline(sandbox.pipeline_repo_name)
    pipeline = list(sandbox.client.inspect_pipeline(sandbox.pipeline_repo_name))[0]
    assert not pipeline.stopped


def test_run_cron():
    sandbox = Sandbox("run_cron")

    # flush the job so it fully finishes
    list(sandbox.client.wait_commit(sandbox.commit.id))

    # this should trigger an error because the sandbox pipeline doesn't have a
    # cron input
    # NOTE: `e` is used after the context
    with pytest.raises(python_pachyderm.RpcError, match=r"pipeline.*have a cron input"):
        sandbox.client.run_cron(sandbox.pipeline_repo_name)


def test_secrets():
    client = python_pachyderm.Client()
    secret_name = util.test_repo_name("test-secrets")

    client.create_secret(
        secret_name,
        {
            "mykey": "my-value",
        },
    )

    secret = client.inspect_secret(secret_name)
    assert secret.secret.name == secret_name

    secrets = client.list_secret()
    assert len(secrets) == 1
    assert secrets[0].secret.name == secret_name

    client.delete_secret(secret_name)

    with pytest.raises(python_pachyderm.RpcError):
        client.inspect_secret(secret_name)

    secrets = client.list_secret()
    assert len(secrets) == 0


def test_get_pipeline_logs():
    sandbox = Sandbox("get_pipeline_logs")
    sandbox.wait()
    # sleep 10 secs to wait for the k8s pod to be ready
    # TODO remove this sleep once we figure out why this test is broken
    # time.sleep(10)

    # Just make sure these spit out some logs
    logs = sandbox.client.get_pipeline_logs(sandbox.pipeline_repo_name)
    assert next(logs) is not None

    logs = sandbox.client.get_pipeline_logs(sandbox.pipeline_repo_name, master=True)
    assert next(logs) is not None


def test_get_job_logs():
    sandbox = Sandbox("get_logs_logs")
    job_id = sandbox.wait()
    pipeline_name = sandbox.pipeline_repo_name

    # Wait for the job to complete
    commit = (pipeline_name, job_id)
    sandbox.client.wait_commit(commit)

    # Just make sure these spit out some logs
    logs = sandbox.client.get_job_logs(pipeline_name, job_id)
    assert next(logs) is not None


def test_create_pipeline():
    client = python_pachyderm.Client()
    client.delete_all()

    input_repo_name = util.create_test_repo(client, "input_repo_test_create_pipeline")

    client.create_pipeline(
        "pipeline_test_create_pipeline",
        transform=pps_proto.Transform(
            cmd=["sh"],
            image="alpine",
            stdin=["cp /pfs/{}/*.dat /pfs/out/".format(input_repo_name)],
        ),
        input=pps_proto.Input(pfs=pps_proto.PFSInput(glob="/*", repo=input_repo_name)),
    )
    assert len(list(client.list_pipeline())) == 1


def test_create_pipeline_from_request():
    client = python_pachyderm.Client()

    repo_name = util.create_test_repo(client, "test_create_pipeline_from_request")
    pipeline_name = util.test_repo_name("test_create_pipeline_from_request")

    # more or less a copy of the opencv demo's edges pipeline spec
    client.create_pipeline_from_request(
        pps_proto.CreatePipelineRequest(
            pipeline=pps_proto.Pipeline(name=pipeline_name),
            description="A pipeline that performs image edge detection by using the OpenCV library.",
            input=pps_proto.Input(
                pfs=pps_proto.PFSInput(
                    glob="/*",
                    repo=repo_name,
                ),
            ),
            transform=pps_proto.Transform(
                cmd=["echo", "hi"],
                image="pachyderm/opencv",
            ),
        )
    )

    assert any(p.pipeline.name == pipeline_name for p in list(client.list_pipeline()))
