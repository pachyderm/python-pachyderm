#!/usr/bin/env python

"""Tests for PPS-related functionality."""

import time

import pytest
import grpc

import python_pachyderm
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

    def wait_for_job(self):
        return util.wait_for_job(self.client, self.commit)


def test_list_job():
    sandbox = Sandbox("list_job")
    job_id = sandbox.wait_for_job()

    jobs = list(sandbox.client.list_job())
    assert len(jobs) >= 1

    jobs = list(sandbox.client.list_job(pipeline_name=sandbox.pipeline_repo_name))
    assert len(jobs) >= 1

    jobs = list(
        sandbox.client.list_job(
            input_commit=(sandbox.input_repo_name, sandbox.commit.id)
        )
    )
    assert len(jobs) >= 1


def test_flush_job():
    sandbox = Sandbox("flush_job")
    jobs = list(sandbox.client.flush_job([sandbox.commit]))
    assert len(jobs) >= 1
    print(jobs[0])


def test_inspect_job():
    sandbox = Sandbox("inspect_job")
    job_id = sandbox.wait_for_job()

    job = sandbox.client.inspect_job(job_id)
    assert job.job.id == job_id


def test_stop_job():
    sandbox = Sandbox("stop_job")
    job_id = sandbox.wait_for_job()

    sandbox.client.stop_job(job_id)
    # This is necessary because `StopJob` does not wait for the job to be killed before returning a result.
    # TODO: remove once this is fixed:
    # https://github.com/pachyderm/pachyderm/issues/3856
    time.sleep(1)
    job = sandbox.client.inspect_job(job_id)
    # We race to stop the job before it finishes - if we lose the race, it will be in state JOB_SUCCESS
    assert (
        job.state == python_pachyderm.JobState.JOB_KILLED.value
        or job.state == python_pachyderm.JobState.JOB_SUCCESS.value
    )


def test_delete_job():
    sandbox = Sandbox("delete_job")
    job_id = sandbox.wait_for_job()
    orig_job_count = len(list(sandbox.client.list_job()))
    sandbox.client.delete_job(job_id)
    assert len(list(sandbox.client.list_job())) == orig_job_count - 1


def test_datums():
    sandbox = Sandbox("datums")
    job_id = sandbox.wait_for_job()

    # flush the job so it fully finishes
    list(sandbox.client.flush_commit([(sandbox.input_repo_name, sandbox.commit.id)]))

    datums = list(sandbox.client.list_datum(job_id))
    assert len(datums) == 1
    datum = sandbox.client.inspect_datum(job_id, datums[0].datum_info.datum.id)
    assert datum.state == python_pachyderm.DatumState.SUCCESS.value

    # Skip this check in >=1.11.0, due to a bug:
    # https://github.com/pachyderm/pachyderm/issues/5123
    # TODO: remove this check once the bug is fixed
    if util.test_pachyderm_version() < (1, 11, 0):
        # Just ensure this doesn't raise an exception
        sandbox.client.restart_datum(job_id)


def test_inspect_pipeline():
    sandbox = Sandbox("inspect_pipeline")
    pipeline = sandbox.client.inspect_pipeline(sandbox.pipeline_repo_name)
    assert pipeline.pipeline.name == sandbox.pipeline_repo_name
    if util.test_pachyderm_version() >= (1, 9, 0):
        pipeline = sandbox.client.inspect_pipeline(
            sandbox.pipeline_repo_name, history=-1
        )
        assert pipeline.pipeline.name == sandbox.pipeline_repo_name


def test_list_pipeline():
    sandbox = Sandbox("list_pipeline")
    pipelines = sandbox.client.list_pipeline()
    assert sandbox.pipeline_repo_name in [
        p.pipeline.name for p in pipelines.pipeline_info
    ]
    pipelines = sandbox.client.list_pipeline(history=-1)
    assert sandbox.pipeline_repo_name in [
        p.pipeline.name for p in pipelines.pipeline_info
    ]


def test_delete_pipeline():
    sandbox = Sandbox("delete_pipeline")
    orig_pipeline_count = len(sandbox.client.list_pipeline().pipeline_info)
    sandbox.client.delete_pipeline(sandbox.pipeline_repo_name)
    assert len(sandbox.client.list_pipeline().pipeline_info) == orig_pipeline_count - 1


def test_delete_all_pipelines():
    sandbox = Sandbox("delete_all_pipelines")
    sandbox.client.delete_all_pipelines()
    pipelines = sandbox.client.list_pipeline()
    assert len(pipelines.pipeline_info) == 0


def test_restart_pipeline():
    sandbox = Sandbox("restart_job")

    sandbox.client.stop_pipeline(sandbox.pipeline_repo_name)
    pipeline = sandbox.client.inspect_pipeline(sandbox.pipeline_repo_name)
    assert pipeline.stopped

    sandbox.client.start_pipeline(sandbox.pipeline_repo_name)
    pipeline = sandbox.client.inspect_pipeline(sandbox.pipeline_repo_name)
    assert not pipeline.stopped


@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_run_pipeline():
    sandbox = Sandbox("run_pipeline")

    # flush the job so it fully finishes
    list(sandbox.client.flush_commit([(sandbox.input_repo_name, sandbox.commit.id)]))

    # just make sure it worked
    sandbox.client.run_pipeline(sandbox.pipeline_repo_name)


@util.skip_if_below_pachyderm_version(1, 9, 10)
def test_run_cron():
    sandbox = Sandbox("run_cron")

    # flush the job so it fully finishes
    list(sandbox.client.flush_commit([(sandbox.input_repo_name, sandbox.commit.id)]))

    # this should trigger an error because the sandbox pipeline doesn't have a
    # cron input
    # NOTE: `e` is used after the context
    with pytest.raises(python_pachyderm.RpcError) as e:
        sandbox.client.run_cron(sandbox.pipeline_repo_name)
    assert "pipeline must have a cron input" in str(e.value)


@util.skip_if_below_pachyderm_version(1, 10, 0)
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
    job_id = sandbox.wait_for_job()

    # Wait for the job to complete
    list(sandbox.client.flush_job([sandbox.commit]))

    # Just make sure these spit out some logs
    logs = sandbox.client.get_pipeline_logs(sandbox.pipeline_repo_name)
    assert next(logs) is not None

    logs = sandbox.client.get_pipeline_logs(sandbox.pipeline_repo_name, master=True)
    assert next(logs) is not None


# job logs are available in 1.8.x, but they frequently fail due to bugs that
# are resolved in 1.9.0
@util.skip_if_below_pachyderm_version(1, 9, 0)
def test_get_job_logs():
    sandbox = Sandbox("get_logs_logs")
    job_id = sandbox.wait_for_job()

    # Wait for the job to complete
    list(sandbox.client.flush_job([sandbox.commit]))

    # Just make sure these spit out some logs
    logs = sandbox.client.get_job_logs(job_id)
    assert next(logs) is not None


def test_create_pipeline_from_request():
    client = python_pachyderm.Client()

    repo_name = util.create_test_repo(client, "test_create_pipeline_from_request")
    pipeline_name = util.test_repo_name("test_create_pipeline_from_request")

    # more or less a copy of the opencv demo's edges pipeline spec
    client.create_pipeline_from_request(
        python_pachyderm.CreatePipelineRequest(
            pipeline=python_pachyderm.Pipeline(name=pipeline_name),
            description="A pipeline that performs image edge detection by using the OpenCV library.",
            input=python_pachyderm.Input(
                pfs=python_pachyderm.PFSInput(
                    glob="/*",
                    repo=repo_name,
                ),
            ),
            transform=python_pachyderm.Transform(
                cmd=["echo", "hi"],
                image="pachyderm/opencv",
            ),
        )
    )

    assert any(
        p.pipeline.name == pipeline_name for p in client.list_pipeline().pipeline_info
    )
