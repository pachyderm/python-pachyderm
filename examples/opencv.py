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
        input=Input(pfs=PFSInput(glob="/*", repo="images"))
    )

    pps_client.create_pipeline(
        pipeline=Pipeline(name="montage"),
        transform=Transform(cmd=["sh"], image="v4tech/imagemagick", stdin=["montage -shadow -background SkyBlue -geometry 300x300+2+2 $(find /pfs -type f | sort) /pfs/out/montage.png"]),
        input=Input(cross=[
            Input(pfs=PFSInput(glob="/", repo="images")),
            Input(pfs=PFSInput(glob="/", repo="edges")),
        ])
    )

    pfs_client.put_file_url(commit=("images", "master"), path="46Q8nDz.jpg", url="http://imgur.com/46Q8nDz.jpg")

    with pfs_client.commit("images", "master") as commit:
        pfs_client.put_file_url(commit=commit, path="g2QnNqa.jpg", url="http://imgur.com/g2QnNqa.jpg")
        pfs_client.put_file_url(commit=commit, path="8MN9Kg0.jpg", url="http://imgur.com/8MN9Kg0.jpg")

if __name__ == '__main__':
    main()
