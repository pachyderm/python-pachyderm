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
            cmd=["python3", "consumer/main.py"],
            image="pachyderm/example-spout101:2.0.0-beta.5",
        ),
        spout=pps_proto.Spout(),
        description="A spout pipeline that emulates the reception of data from an external source",
    )

    client.create_pipeline(
        pipeline_name="processor",
        transform=pps_proto.Transform(
            cmd=["python3", "processor/main.py"],
            image="pachyderm/example-spout101:2.0.0-beta.5",
        ),
        input=pps_proto.Input(
            pfs=pps_proto.PFSInput(repo="spout", branch="master", glob="/*")
        ),
        description="A pipeline that sorts 1KB vs 2KB files",
    )

    client.create_pipeline(
        pipeline_name="reducer",
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
        description="A pipeline that reduces 1K/ and 2K/ directories",
    )


if __name__ == "__main__":
    main()
