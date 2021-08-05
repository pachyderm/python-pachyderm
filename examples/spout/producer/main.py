#!/usr/bin/env python3

import time
import python_pachyderm


def main():
    manager = python_pachyderm.SpoutManager()
    contents = b"#"

    while True:
        with manager.commit() as commit:
            commit.put_file_from_bytes("content", contents)

        time.sleep(5.0)


if __name__ == "__main__":
    main()
