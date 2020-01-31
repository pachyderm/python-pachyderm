#!/usr/bin/env python3

import time
import python_pachyderm

def main():
    with python_pachyderm.SpoutProducer() as spout:
        while True:
            spout.add_from_bytes("foo", b"bar")
            time.sleep(1.0)

if __name__ == "__main__":
    main()
