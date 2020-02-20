#!/usr/bin/env python3

import os
import python_pachyderm

def relpath(path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)

def main():
    client = python_pachyderm.Client()

    client.create_pipeline(
        pipeline_name="producer",
        transform=python_pachyderm.Transform(
            cmd=["python3", "/app/main.py"],
            image="ysimonson/pachyderm_spout_producer",
        ),
        spout=python_pachyderm.Spout(
            overwrite=False,
            marker="marker",
        ),
    )

    python_pachyderm.create_python_pipeline(
        client,
        relpath("consumer"),
        input=python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/", repo="producer")),
    )

if __name__ == '__main__':
    main()
