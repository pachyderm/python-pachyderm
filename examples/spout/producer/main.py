#!/usr/bin/env python3

import time
import python_pachyderm

def add():
    with python_pachyderm.SpoutManager() as spout:
        spout.add_from_bytes("foo", b"#")

def main():
    while True:
        python_pachyderm.retry(add)
        time.sleep(5.0)

if __name__ == "__main__":
    main()
