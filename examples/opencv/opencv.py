#!/usr/bin/env python3

# This is a reproduction of pachyderm's opencv example in python. A
# walkthrough is available in the pachyderm docs:
# https://docs.pachyderm.io/en/latest/getting_started/beginner_tutorial.html
#
# It makes heavy use of python_pachyderm's higher-level utility functionality
# (`create_python_pipeline`, `put_files`), as well as more run-of-the-mill
# functionality (`create_repo`, `create_pipeline`).

import os
import shutil
import tempfile
import python_pachyderm
from python_pachyderm.service import pps_proto


def relpath(path):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), path)


def main():
    # Connects to a pachyderm cluster on the default host:port
    # (`localhost:30650`). This will work for certain environments (e.g. k8s
    # running on docker for mac), as well as when port forwarding is being
    # used. For other setups, you'll want one of the alternatives:
    # 1) To connect to pachyderm when this script is running inside the
    #    cluster, use `python_pachyderm.Client.new_in_cluster()`.
    # 2) To connect to pachyderm via a pachd address, use
    #    `python_pachyderm.Client.new_from_pachd_address`.
    # 3) To explicitly set the host and port, pass parameters into
    #   `python_pachyderm.Client()`.
    client = python_pachyderm.Client()

    # Create a repo called images
    client.create_repo("images")

    # Create the edges pipeline (and the edges repo automatically). This
    # pipeline runs when data is committed to the images repo, as indicated
    # by the input field.
    client.create_pipeline(
        "edges",
        transform=pps_proto.Transform(
            cmd=["python3", "/edges.py"],
            image="pachyderm/opencv",
        ),
        input=pps_proto.Input(pfs=pps_proto.PFSInput(repo="images", glob="/*")),
    )

    # Create the montage pipeline (and the montage repo automatically). This
    # pipeline runs when data is committed to either the images repo or edges
    # repo, as indicated by the input field.
    client.create_pipeline(
        "montage",
        transform=pps_proto.Transform(
            cmd=["sh"],
            image="v4tech/imagemagick",
            stdin=[
                "montage -shadow -background SkyBlue -geometry 300x300+2+2 $(find /pfs ! -name .env -type f | sort) /pfs/out/montage.png"
            ]
        ),
        input=pps_proto.Input(
            cross=[
                pps_proto.Input(pfs=pps_proto.PFSInput(glob="/", repo="images")),
                pps_proto.Input(pfs=pps_proto.PFSInput(glob="/", repo="edges")),
            ]
        ),
    )

    with client.commit("images", "master") as commit:
        # Add some images, recursively inserting content from the images
        # directory. Alternatively, you could use `client.put_file_url` or
        # `client_put_file_bytes`.
        python_pachyderm.put_files(client, relpath("images"), commit, "/")

    # Wait for the commit (and its downstream commits) to finish
    client.wait_commit(("montage", "master"))

    # Get the montage
    source_file = client.get_file(("montage", "master"), "/montage.png")
    with tempfile.NamedTemporaryFile(suffix="montage.png", delete=False) as dest_file:
        shutil.copyfileobj(source_file, dest_file)
        print("montage written to {}".format(dest_file.name))


if __name__ == "__main__":
    main()
