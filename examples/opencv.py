#!/usr/bin/env python3

# This is a reproduction of pachyderm's opencv example in python. A
# walkthrough is available in the pachyderm docs:
# https://docs.pachyderm.io/en/latest/getting_started/beginner_tutorial.html

# TODO: remove
from python_pachyderm.client.pps.pps_pb2 import *
from python_pachyderm.client.pps.pps_pb2_grpc import *

import python_pachyderm

def main():
    pfs_client = python_pachyderm.PfsClient()
    pps_client = python_pachyderm.PpsClient()

    pfs_client.create_repo("images")

    pps_client.create_pipeline(
        pipeline=Pipeline(name="edges"),
        transform=Transform(cmd=["python3", "edges.py"], image="pachyderm/opencv"),
        parallelism_spec=None,
        hashtree_spec=None,
        egress=None,
        update=None,
        output_branch=None,
        scale_down_threshold=None,
        resource_requests=None,
        resource_limits=None,
        input=Input(pfs=PFSInput(glob="/*", repo="images")),
        description=None,
        cache_size=None,
        enable_stats=None,
        reprocess=None,
        batch=None,
        max_queue_size=None,
        service=None,
        chunk_spec=None,
        datum_timeout=None,
        job_timeout=None,
        salt=None,
        standby=None,
        datum_tries=None,
        scheduling_spec=None,
        pod_spec=None,
        pod_patch=None,
    )

    pps_client.create_pipeline(
        pipeline=Pipeline(name="montage"),
        transform=Transform(cmd=["sh"], image="v4tech/imagemagick", stdin=["montage -shadow -background SkyBlue -geometry 300x300+2+2 $(find /pfs -type f | sort) /pfs/out/montage.png"]),
        parallelism_spec=None,
        hashtree_spec=None,
        egress=None,
        update=None,
        output_branch=None,
        scale_down_threshold=None,
        resource_requests=None,
        resource_limits=None,
        input=Input(cross=[
            Input(pfs=PFSInput(glob="/", repo="images")),
            Input(pfs=PFSInput(glob="/", repo="edges")),
        ]),
        description=None,
        cache_size=None,
        enable_stats=None,
        reprocess=None,
        batch=None,
        max_queue_size=None,
        service=None,
        chunk_spec=None,
        datum_timeout=None,
        job_timeout=None,
        salt=None,
        standby=None,
        datum_tries=None,
        scheduling_spec=None,
        pod_spec=None,
        pod_patch=None,
    )

if __name__ == '__main__':
    main()
