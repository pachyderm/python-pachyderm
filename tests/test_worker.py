from typing import Callable

import os 

from python_pachyderm import Client
from python_pachyderm.proto.v2.pps import pps_pb2

from .test_pfs import _client_fixture, _repo_fixture

IMAGE_NAME = os.environ.get("DATUM_BATCHING_IMAGE")
# IMAGE_NAME = "bonenfan5ben/datum_batching:0037pp"  # TODO: Don't do this.


def generate_stdin(func: Callable[[], None]):
    """Generates the stdin field of for the test pipelines.

    Args:
        func: The function containing the "user code" of the test pipeline.
          This must be defined at the root of the script, i.e. not as a
          method to a class or defined within another function.
    """
    from inspect import getsource
    from textwrap import dedent

    test_script = (
        f"{dedent(getsource(func))}\n\n"
        'if __name__ == "__main__":\n'
        f"    {func.__name__}()\n"
    )
    return [
        f"echo '{test_script}' > main.py",
        "python3 main.py",
    ]


def test_datum_batching(client: Client, repo: str):
    """Test that exceptions within the user code is caught, reported to the
    worker binary, and iteration continues.

    This test uploads 10 files to the input repo and the user code
      copies each file to the output repo. The pipeline job should finish
      successfully and the output repo should contain 10 files.
    """

    def user_code():
        """Assert the one file is mounted in /pfs/batch_datums_input
        and copy it to /pfs/out.
        """
        import os
        import shutil
        from python_pachyderm import batch_all_datums

        @batch_all_datums
        def main():
            datum_files = os.listdir("/pfs/batch_datums_input")
            print(datum_files)
            assert len(datum_files) == 1
            shutil.copy(
                f"/pfs/batch_datums_input/{datum_files[0]}",
                f"/pfs/out/{datum_files[0]}",
            )

        return main()

    input_files = [f"/file_{i:02d}.dat" for i in range(10)]
    with client.commit(repo, "master") as commit:
        for file in input_files:
            client.put_file_bytes(commit, file, b"DATA")

    pipeline_name = "datum_batching_pipeline"
    try:
        client.create_pipeline(
            pipeline_name=pipeline_name,
            input=pps_pb2.Input(
                pfs=pps_pb2.PFSInput(
                    repo=repo,
                    name="batch_datums_input",  # Name explicitly set for user code.
                    glob="/*",
                )
            ),
            transform=pps_pb2.Transform(
                cmd=[
                    "bash",
                ],
                datum_batching=True,
                image=IMAGE_NAME,
                stdin=generate_stdin(user_code),
            ),
        )
        job_info = next(client.list_job(pipeline_name))
        next(client.inspect_job(job_info.job.id, pipeline_name, wait=True))

        output_files = client.list_file(job_info.output_commit, path="/")
        assert len(list(output_files)) == len(input_files)
    finally:  # Cleanup our manually defined test pipeline.
        client.delete_pipeline(pipeline_name, force=True)


def test_datum_batching_errors(client: Client, repo: str):
    """Test that exceptions within the user code is caught, reported to the
    worker binary, and iteration continues.

    This test uploads a single file to the input repo and the user code
      always raises an exception. The pipeline has err_cmd set to "true", so
      the pipeline job should finish successfully and the output repo should
      be empty.
    """

    def user_code_errors():
        """Raises an Exception for every datum."""
        from python_pachyderm import Client

        worker = Client().worker
        while True:
            with worker.batch_datum():
                raise Exception("Something Bad Happened!")

    with client.commit(repo, "master") as commit:
        client.put_file_bytes(commit, path="/file.dat", value=b"DATA")

    pipeline_name = "datum_batching_pipeline_error"
    try:
        client.create_pipeline(
            pipeline_name=pipeline_name,
            input=pps_pb2.Input(
                pfs=pps_pb2.PFSInput(
                    repo=repo,
                    name="batch_datums_input",  # Name explicitly set for user code.
                    glob="/*",
                )
            ),
            transform=pps_pb2.Transform(
                cmd=[
                    "bash",
                ],
                err_cmd=[
                    "true",
                ],  # Note err_cmd set.
                datum_batching=True,
                image=IMAGE_NAME,
                stdin=generate_stdin(user_code_errors),
            ),
        )
        started_job = next(client.list_job(pipeline_name))
        completed_job = next(
            client.inspect_job(started_job.job.id, pipeline_name, wait=True)
        )
        assert completed_job.state == pps_pb2.JobState.JOB_SUCCESS

        output_files = client.list_file(started_job.output_commit, path="/")
        assert len(list(output_files)) == 0
    finally:  # Cleanup our manually defined test pipeline.
        # pass
        client.delete_pipeline(pipeline_name, force=True)
