#!/usr/bin/env python3

import os
import python_pachyderm

def relpath(path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)

def main():
    client = python_pachyderm.Client()

    client.create_pipeline(
        pipeline_name="printer",
        transform=python_pachyderm.Transform(
            cmd=["python3", "/app/main.py"],
            image="ysimonson/pachyderm_spout_printer",
        ),
        spout=python_pachyderm.Spout(overwrite=False),
    )

    python_pachyderm.create_python_pipeline(
        client,
        relpath("echoer"),
        input=python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/", repo="printer")),
    )

    # jobs = list(client.list_job(pipeline_name="echoer"))
    # print(jobs)

if __name__ == '__main__':
    main()
