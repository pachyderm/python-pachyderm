#!/usr/bin/env python3

# This is a reproduction of pachyderm's opencv example in python. A
# walkthrough is available in the pachyderm docs:
# https://docs.pachyderm.io/en/latest/getting_started/beginner_tutorial.html

from python_pachyderm import pfs, pps

def main():
    pfs_client = pfs.PfsClient()
    pps_client = pps.PpsClient()

    pfs_client.create_repo("images")

    pps_client.create_pipeline(
        "edges",
        transform=pps.Transform(cmd=["python3", "edges.py"], image="pachyderm/opencv"),
        input=pps.Input(pfs=pps.PFSInput(glob="/*", repo="images"))
    )

    pps_client.create_pipeline(
        "montage",
        transform=pps.Transform(cmd=["sh"], image="v4tech/imagemagick", stdin=["montage -shadow -background SkyBlue -geometry 300x300+2+2 $(find /pfs -type f | sort) /pfs/out/montage.png"]),
        input=pps.Input(cross=[
            pps.Input(pfs=pps.PFSInput(glob="/", repo="images")),
            pps.Input(pfs=pps.PFSInput(glob="/", repo="edges")),
        ])
    )

    pfs_client.put_file_url("images/master", "46Q8nDz.jpg", "http://imgur.com/46Q8nDz.jpg")

    with pfs_client.commit("images", "master") as commit:
        pfs_client.put_file_url(commit, "g2QnNqa.jpg", "http://imgur.com/g2QnNqa.jpg")
        pfs_client.put_file_url(commit, "8MN9Kg0.jpg", "http://imgur.com/8MN9Kg0.jpg")

if __name__ == '__main__':
    main()
