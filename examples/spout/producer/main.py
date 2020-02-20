#!/usr/bin/env python3

import time
import python_pachyderm

MAX_ATTEMPTS = 10

def append_to_spout():
    # note: the marker filename is set when creating a pipeline,
    # i.e. in `../main.py`.
    with python_pachyderm.SpoutManager(marker_filename="marker") as spout:
        try:
            with spout.marker() as marker_file:
                contents = marker_file.read()
        except:
            contents = b""

        # append a # to the existing contents, pulled from the marker
        contents += b"#"

        spout.add_marker_from_bytes(contents)
        spout.add_from_bytes("content", contents)

def main():
    while True:
        attempts = 0

        # run this a few times, in case the spout isn't setup yet
        while True:
            try:
                append_to_spout()
                break
            except:
                attempts += 1
                if attempts == MAX_ATTEMPTS:
                    raise
            
            time.sleep(1.0)

        time.sleep(1.0)

if __name__ == "__main__":
    main()
