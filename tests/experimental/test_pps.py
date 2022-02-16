#!/usr/bin/env python

"""Tests for PPS-related functionality."""

import time

from grpclib.exceptions import GRPCError
import pytest

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.pps import (
    CreatePipelineRequest,
    DatumState,
    Input,
    JobState,
    PfsInput,
    Pipeline,
    Transform,
)
from tests.experimental import util

# bp_to_pb: PfsInput -> PFSInput


class Sandbox:
    def __init__(self, test_name):
        client = ExperimentalClient()
        client.delete_all()
        commit, input_repo_name, pipeline_repo_name = util.create_test_pipeline(
            client, test_name
        )
        self.client = client
        self.commit = commit
        self.input_repo_name = input_repo_name
        self.pipeline_repo_name = pipeline_repo_name

    def wait(self):
        return self.client.pfs.wait_commit(self.commit.id)[0].commit.id


def test_list_subjob():
    sandbox = Sandbox("list_subjob")
    sandbox.wait()

    jobs = list(sandbox.client.pps.list_job())
    assert len(jobs) >= 1

    jobs = list(sandbox.client.pps.list_job(pipeline_name=sandbox.pipeline_repo_name))
    assert len(jobs) >= 1

    jobs = list(
        sandbox.client.pps.list_job(
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
    client.pfs.wait_commit(commit.id)

    jobs = list(client.pps.list_job())
    assert len(jobs) == 4


def test_inspect_subjob():
    sandbox = Sandbox("inspect_subjob")
    job_id = sandbox.wait()

    job_info = list(sandbox.client.pps.inspect_job(job_id, sandbox.pipeline_repo_name))
    assert job_info[0].job.id == job_id


def test_inspect_job():
    sandbox = Sandbox("inspect_job1")
    sandbox.wait()
    client = sandbox.client

    commit, _, _ = util.create_test_pipeline(client, "inspect_job2")
    client.pfs.wait_commit(commit.id)

    jobs = list(client.pps.list_job())
    job1 = list(client.pps.inspect_job(job_id=jobs[0].job_set.id))

    assert job1[0].job.id == commit.id
    assert len(jobs) == 4


def test_stop_job():
    sandbox = Sandbox("stop_job")
    pipeline_name = sandbox.pipeline_repo_name
    job_id = sandbox.wait()

    sandbox.client.pps.stop_job(job_id, pipeline_name)
    # This is necessary because `StopJob` does not wait for the job to be
    # killed before returning a result.
    # TODO: remove once this is fixed:
    # https://github.com/pachyderm/pachyderm/issues/3856
    time.sleep(1)
    job_info = list(sandbox.client.pps.inspect_job(job_id, pipeline_name))
    # We race to stop the job before it finishes - if we lose the race, it will
    # be in state JOB_SUCCESS
    assert job_info[0].state in (JobState.JOB_KILLED, JobState.JOB_SUCCESS)


def test_delete_job():
    sandbox = Sandbox("delete_job")
    job_id = sandbox.wait()
    orig_job_count = len(list(sandbox.client.pps.list_job()))
    sandbox.client.pps.delete_job(job_id, sandbox.pipeline_repo_name)
    jobs = len(list(sandbox.client.pps.list_job()))
    assert jobs == orig_job_count - 1


def test_datums():
    sandbox = Sandbox("datums")
    pipeline_name = sandbox.pipeline_repo_name
    job_id = sandbox.wait()

    # flush the job so it fully finishes
    list(sandbox.client.pfs.wait_commit(sandbox.commit.id))

    datums = list(sandbox.client.pps.list_datum(pipeline_name, job_id))
    assert len(datums) == 1
    datum = sandbox.client.pps.inspect_datum(pipeline_name, job_id, datums[0].datum.id)
    assert datum.state == DatumState.SUCCESS

    error_message_match = (
        rf"datum matching filter \[.*\] could not be found for job ID {job_id}"
    )
    with pytest.raises(GRPCError, match=error_message_match):
        sandbox.client.pps.restart_datum(pipeline_name, job_id)


def test_inspect_pipeline():
    sandbox = Sandbox("inspect_pipeline")
    pipeline = list(sandbox.client.pps.inspect_pipeline(sandbox.pipeline_repo_name))[0]
    assert pipeline.pipeline.name == sandbox.pipeline_repo_name
    pipelines = list(
        sandbox.client.pps.inspect_pipeline(sandbox.pipeline_repo_name, history=-1)
    )
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines]


def test_list_pipeline():
    sandbox = Sandbox("list_pipeline")
    pipelines = list(sandbox.client.pps.list_pipeline())
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines]
    pipelines = list(sandbox.client.pps.list_pipeline(history=-1))
    assert sandbox.pipeline_repo_name in [p.pipeline.name for p in pipelines]


def test_delete_pipeline():
    sandbox = Sandbox("delete_pipeline")
    orig_pipeline_count = len(list(sandbox.client.pps.list_pipeline()))
    sandbox.client.pps.delete_pipeline(sandbox.pipeline_repo_name)
    assert len(list(sandbox.client.pps.list_pipeline())) == orig_pipeline_count - 1


def test_delete_all_pipelines():
    sandbox = Sandbox("delete_all_pipelines")
    sandbox.client.pps.delete_all_pipelines()
    pipelines = list(sandbox.client.pps.list_pipeline())
    assert len(pipelines) == 0


def test_restart_pipeline():
    sandbox = Sandbox("restart_job")

    sandbox.client.pps.stop_pipeline(sandbox.pipeline_repo_name)
    pipeline = list(sandbox.client.pps.inspect_pipeline(sandbox.pipeline_repo_name))[0]
    assert pipeline.stopped

    sandbox.client.pps.start_pipeline(sandbox.pipeline_repo_name)
    pipeline = list(sandbox.client.pps.inspect_pipeline(sandbox.pipeline_repo_name))[0]
    assert not pipeline.stopped


def test_run_cron():
    sandbox = Sandbox("run_cron")

    # flush the job so it fully finishes
    list(sandbox.client.pfs.wait_commit(sandbox.commit.id))

    # this should trigger an error because the sandbox pipeline doesn't have a
    # cron input
    # NOTE: `e` is used after the context
    with pytest.raises(GRPCError, match=r"pipeline.*have a cron input"):
        sandbox.client.pps.run_cron(sandbox.pipeline_repo_name)


def test_secrets():
    client = ExperimentalClient()
    secret_name = util.test_repo_name("test-secrets")

    client.pps.create_secret(secret_name, {"mykey": "my-value"})

    secret = client.pps.inspect_secret(secret_name)
    assert secret.secret.name == secret_name

    secrets = client.pps.list_secret()
    assert len(secrets) == 1
    assert secrets[0].secret.name == secret_name

    client.pps.delete_secret(secret_name)

    with pytest.raises(GRPCError):
        client.pps.inspect_secret(secret_name)

    secrets = client.pps.list_secret()
    assert len(secrets) == 0


def test_get_pipeline_logs():
    sandbox = Sandbox("get_pipeline_logs")
    sandbox.wait()

    # Just make sure these spit out some logs
    logs = sandbox.client.pps.get_pipeline_logs(sandbox.pipeline_repo_name)
    assert next(logs) is not None

    logs = sandbox.client.pps.get_pipeline_logs(sandbox.pipeline_repo_name, master=True)
    assert next(logs) is not None


def test_get_job_logs():
    sandbox = Sandbox("get_logs_logs")
    job_id = sandbox.wait()
    pipeline_name = sandbox.pipeline_repo_name

    # Wait for the job to complete
    commit = (pipeline_name, job_id)
    sandbox.client.pfs.wait_commit(commit)

    # Just make sure these spit out some logs
    logs = sandbox.client.pps.get_job_logs(pipeline_name, job_id)
    assert next(logs) is not None


def test_create_pipeline():
    client = ExperimentalClient()
    client.delete_all()

    input_repo_name = util.create_test_repo(client, "input_repo_test_create_pipeline")

    client.pps.create_pipeline(
        "pipeline_test_create_pipeline",
        transform=Transform(
            cmd=["sh"],
            image="alpine",
            stdin=["cp /pfs/{}/*.dat /pfs/out/".format(input_repo_name)],
        ),
        input=Input(pfs=PfsInput(glob="/*", repo=input_repo_name)),
    )
    assert len(list(client.pps.list_pipeline())) == 1


def test_create_pipeline_from_request():
    client = ExperimentalClient()

    repo_name = util.create_test_repo(client, "test_create_pipeline_from_request")
    pipeline_name = util.test_repo_name("test_create_pipeline_from_request")

    # more or less a copy of the opencv demo's edges pipeline spec
    client.pps.create_pipeline_from_request(
        CreatePipelineRequest(
            pipeline=Pipeline(name=pipeline_name),
            description="A pipeline that performs image edge detection by using the OpenCV library.",
            input=Input(
                pfs=PfsInput(
                    glob="/*",
                    repo=repo_name,
                ),
            ),
            transform=Transform(
                cmd=["echo", "hi"],
                image="pachyderm/opencv",
            ),
        )
    )

    assert any(
        p.pipeline.name == pipeline_name for p in list(client.pps.list_pipeline())
    )
