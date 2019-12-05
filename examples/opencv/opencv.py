#!/usr/bin/env python3

# This is a reproduction of pachyderm's opencv example in python. A
# walkthrough is available in the pachyderm docs:
# https://docs.pachyderm.io/en/latest/getting_started/beginner_tutorial.html

import os
import python_pachyderm

def main():
    client = python_pachyderm.Client()

    client.create_repo("images")

    python_pachyderm.build_pipeline(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "edges"),
        "ysimonson/opencv",
        input=python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/*", repo="images"))
    )

    client.create_pipeline(
        "montage",
        transform=python_pachyderm.Transform(cmd=["sh"], image="v4tech/imagemagick", stdin=["montage -shadow -background SkyBlue -geometry 300x300+2+2 $(find /pfs -type f | sort) /pfs/out/montage.png"]),
        input=python_pachyderm.Input(cross=[
            python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/", repo="images")),
            python_pachyderm.Input(pfs=python_pachyderm.PFSInput(glob="/", repo="edges")),
        ])
    )

    client.put_file_url("images/master", "46Q8nDz.jpg", "http://imgur.com/46Q8nDz.jpg")

    with client.commit("images", "master") as commit:
        client.put_file_url(commit, "g2QnNqa.jpg", "http://imgur.com/g2QnNqa.jpg")
        client.put_file_url(commit, "8MN9Kg0.jpg", "http://imgur.com/8MN9Kg0.jpg")

if __name__ == '__main__':
    main()
