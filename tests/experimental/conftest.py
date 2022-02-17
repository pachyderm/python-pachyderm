import pytest

from python_pachyderm.experimental import Client as ExperimentalClient
from python_pachyderm.experimental.mixin.pps import Input, PfsInput, Transform


@pytest.fixture(name="client")
def _client_fixture():
    client = ExperimentalClient()
    return client


@pytest.fixture(name="repo")
def _repo_fixture(request, client: ExperimentalClient) -> str:
    """Create a repository name from the test function name."""
    repo = (
        request.node.nodeid.split("[")[0]
        .replace("/", "-")
        .replace(":", "-")
        .replace(".py", "")
    )
    client.pfs.delete_repo(repo, force=True)
    client.pfs.create_repo(repo, "test repo for python_pachyderm")
    yield repo
    client.pfs.delete_repo(repo, force=True)


@pytest.fixture(name="pipeline")
def _pipeline_fixture(client: ExperimentalClient, repo: str) -> str:
    """Create a pipeline."""
    pipeline_name = f"{repo}-pipeline"
    client.pps.delete_pipeline(pipeline_name=pipeline_name, force=True)
    client.pps.create_pipeline(
        pipeline_name,
        transform=Transform(
            cmd=["sh"],
            image="alpine",
            stdin=[f"cp /pfs/{repo}/*.dat /pfs/out/"],
        ),
        input=Input(pfs=PfsInput(glob="/*", repo=repo)),
    )
    yield pipeline_name
    client.pps.delete_pipeline(pipeline_name=pipeline_name, force=True)
