#!/usr/bin/env python3

import os
import python_pachyderm
from python_pachyderm.service import pps_proto


def relpath(path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)


def main():
    client = python_pachyderm.Client()

    client.create_pipeline(
        pipeline_name="spout",
        transform=pps_proto.Transform(
            cmd=["python3", "/app/poll_consumer.py"],
            image="pachyderm/example-python-spout-consumer",
        ),
        spout=pps_proto.Spout(),
    )

    client.create_pipeline(
        pipeline_name="processor",
        transform=pps_proto.Transform(
            cmd=["python3", "/app/log_messages.py"],
            image="pachyderm/example-python-spout-processor",
        ),
        input=pps_proto.Input(
            pfs=pps_proto.PFSInput(repo="spout", branch="master", glob="/*")
        ),
    )

    # concats the files in the 1K and 2K directories in the consumer repo
    client.create_pipeline(
        pipeline_name="reduce",
        transform=pps_proto.Transform(
            cmd=["bash"],
            stdin=[
                "set -x",
                "FILES=/pfs/processor/*/*",
                "for f in $FILES",
                "do",
                "directory=`dirname $f`",
                "out=`basename $directory`",
                "cat $f >> /pfs/out/${out}.txt",
                "done",
            ],
        ),
        input=pps_proto.Input(
            pfs=pps_proto.PFSInput(repo="processor", branch="master", glob="/*")
        ),
    )


if __name__ == "__main__":
    main()
