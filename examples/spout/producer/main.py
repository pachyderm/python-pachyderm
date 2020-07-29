#!/usr/bin/env python3

import time
import python_pachyderm


def main():
    # note: the marker filename is set when creating a pipeline, i.e. in
    # `../main.py`.
    manager = python_pachyderm.SpoutManager(marker_filename="marker")
    
    while True:
        try:
            with manager.marker() as marker_file:
                contents = marker_file.read()
        except:
            contents = b""

        # append a # to the existing contents, pulled from the marker
        contents += b"#"

        with manager.commit() as commit:
            commit.put_marker_from_bytes(contents)
            commit.put_file_from_bytes("content", contents)

        time.sleep(5.0)

if __name__ == "__main__":
    main()
